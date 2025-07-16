
use pq_sys::*;
use std::ffi::{CStr, CString};
use std::ptr;
use std::thread;
use std::time::Duration;

fn print_error(error_msg: &mut Option<String>, message: Option<&str>) {
    *error_msg = message.map(|s| s.to_string());
}

enum Action {
    Continue,
    ReturnMinusOne,
}

fn handle_inner_error(
    attempt: i32,
    max_retries: i32,
    inner_error: &Option<String>,
    path: &str,
    error_msg: &mut Option<String>,
    is_lock_fail: bool,
) -> Action {
    if let Some(err) = inner_error {
        if err.contains("No records found") {
            print_error(error_msg, Some(err));
            Action::ReturnMinusOne
        } else if attempt < max_retries {
            Action::Continue
        } else {
            let msg = format!("Error pushing stream data for path '{}': {}", path, err);
            print_error(error_msg, Some(&msg));
            Action::ReturnMinusOne
        }
    } else if is_lock_fail {
        if attempt < max_retries {
            Action::Continue
        } else {
            let msg = format!("Could not lock any row for path='{}' after {} attempts", path, max_retries);
            print_error(error_msg, Some(&msg));
            Action::ReturnMinusOne
        }
    } else {
        print_error(error_msg, Some("Unexpected error in push_stream_data"));
        Action::ReturnMinusOne
    }
}

fn rollback(conn: *mut PGconn) {
    let rb_str = CString::new("ROLLBACK").unwrap();
    let rb_res = unsafe { PQexec(conn, rb_str.as_ptr()) };
    unsafe { PQclear(rb_res) };
}

pub fn push_stream_data(
    conn: *mut PGconn,
    base_table: &str,
    path: &str,
    data: &str,
    max_retries: i32,
    retry_delay: f64,
    error_msg: &mut Option<String>,
) -> i32 {
    print_error(error_msg, None);

    if path.is_empty() {
        print_error(error_msg, Some("Path cannot be empty or None"));
        return -1;
    }

    let path_cstr = CString::new(path).unwrap();
    let param_values: [*const i8; 1] = [path_cstr.as_ptr()];

    let mut inner_error: Option<String>;

    for attempt in 1..=max_retries {
        inner_error = None;

        let begin_str = CString::new("BEGIN").unwrap();
        let res = unsafe { PQexec(conn, begin_str.as_ptr()) };
        if unsafe { PQresultStatus(res) } != PGRES_COMMAND_OK {
            let err = unsafe { CStr::from_ptr(PQerrorMessage(conn)).to_string_lossy().to_string() };
            print_error(error_msg, Some(&err));
            unsafe { PQclear(res) };
            return -1;
        }
        unsafe { PQclear(res) };

        // 1) ensure there's at least one record to update
        let query_buf = format!("SELECT COUNT(*) as count FROM {} WHERE path = $1", base_table);
        let c_query = CString::new(query_buf).unwrap();
        let res = unsafe {
            PQexecParams(
                conn,
                c_query.as_ptr(),
                1,
                ptr::null(),
                param_values.as_ptr(),
                ptr::null(),
                ptr::null(),
                0,
            )
        };
        if unsafe { PQresultStatus(res) } != PGRES_TUPLES_OK {
            inner_error = Some(unsafe { CStr::from_ptr(PQerrorMessage(conn)).to_string_lossy().to_string() });
            unsafe { PQclear(res) };
            rollback(conn);
            let action = handle_inner_error(attempt, max_retries, &inner_error, path, error_msg, false);
            if let Action::Continue = action {
                thread::sleep(Duration::from_secs_f64(retry_delay));
                continue;
            } else {
                return -1;
            }
        }
        let total: i32 = unsafe { CStr::from_ptr(PQgetvalue(res, 0, 0)).to_str().unwrap().parse().unwrap() };
        unsafe { PQclear(res) };
        if total == 0 {
            inner_error = Some(format!(
                "No records found for path='{}'. Records must be pre-allocated for stream tables.",
                path
            ));
            rollback(conn);
            let action = handle_inner_error(attempt, max_retries, &inner_error, path, error_msg, false);
            if let Action::Continue = action {
                thread::sleep(Duration::from_secs_f64(retry_delay));
                continue;
            } else {
                return -1;
            }
        }

        // 2) try to lock the oldest record regardless of valid status (true circular buffer)
        let query_buf = format!(
            "SELECT id FROM {} WHERE path = $1 ORDER BY recorded_at ASC FOR UPDATE SKIP LOCKED LIMIT 1",
            base_table
        );
        let c_query = CString::new(query_buf).unwrap();
        let res = unsafe {
            PQexecParams(
                conn,
                c_query.as_ptr(),
                1,
                ptr::null(),
                param_values.as_ptr(),
                ptr::null(),
                ptr::null(),
                0,
            )
        };
        if unsafe { PQresultStatus(res) } != PGRES_TUPLES_OK {
            inner_error = Some(unsafe { CStr::from_ptr(PQerrorMessage(conn)).to_string_lossy().to_string() });
            unsafe { PQclear(res) };
            rollback(conn);
            let action = handle_inner_error(attempt, max_retries, &inner_error, path, error_msg, false);
            if let Action::Continue = action {
                thread::sleep(Duration::from_secs_f64(retry_delay));
                continue;
            } else {
                return -1;
            }
        }
        if unsafe { PQntuples(res) } == 0 {
            unsafe { PQclear(res) };
            rollback(conn);
            let action = handle_inner_error(attempt, max_retries, &None, path, error_msg, true);
            if let Action::Continue = action {
                thread::sleep(Duration::from_secs_f64(retry_delay));
                continue;
            } else {
                return -1;
            }
        }

        let record_id = unsafe { CStr::from_ptr(PQgetvalue(res, 0, 0)).to_str().unwrap().to_string() };
        unsafe { PQclear(res) };

        // 3) perform the update with valid=TRUE (always overwrites oldest record)
        let query_buf = format!(
            "UPDATE {} SET data = $1, recorded_at = NOW(), valid = TRUE WHERE id = $2 RETURNING id",
            base_table
        );
        let c_query = CString::new(query_buf).unwrap();
        let data_cstr = CString::new(data).unwrap();
        let record_id_cstr = CString::new(record_id).unwrap();
        let update_params: [*const i8; 2] = [data_cstr.as_ptr(), record_id_cstr.as_ptr()];
        let res = unsafe {
            PQexecParams(
                conn,
                c_query.as_ptr(),
                2,
                ptr::null(),
                update_params.as_ptr(),
                ptr::null(),
                ptr::null(),
                0,
            )
        };
        let status = unsafe { PQresultStatus(res) };
        if status != PGRES_TUPLES_OK || unsafe { PQntuples(res) } != 1 {
            inner_error = if status != PGRES_TUPLES_OK {
                Some(unsafe { CStr::from_ptr(PQerrorMessage(conn)).to_string_lossy().to_string() })
            } else {
                Some("Failed to update record".to_string())
            };
            unsafe { PQclear(res) };
            rollback(conn);
            let action = handle_inner_error(attempt, max_retries, &inner_error, path, error_msg, false);
            if let Action::Continue = action {
                thread::sleep(Duration::from_secs_f64(retry_delay));
                continue;
            } else {
                return -1;
            }
        }
        unsafe { PQclear(res) };

        let commit_str = CString::new("COMMIT").unwrap();
        let res = unsafe { PQexec(conn, commit_str.as_ptr()) };
        if unsafe { PQresultStatus(res) } != PGRES_COMMAND_OK {
            inner_error = Some(unsafe { CStr::from_ptr(PQerrorMessage(conn)).to_string_lossy().to_string() });
            unsafe { PQclear(res) };
            rollback(conn);
            let action = handle_inner_error(attempt, max_retries, &inner_error, path, error_msg, false);
            if let Action::Continue = action {
                thread::sleep(Duration::from_secs_f64(retry_delay));
                continue;
            } else {
                return -1;
            }
        }
        unsafe { PQclear(res) };

        return 0;
    }

    print_error(error_msg, Some("Unexpected error in push_stream_data"));
    -1
}
