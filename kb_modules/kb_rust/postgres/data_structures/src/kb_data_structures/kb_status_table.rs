
use pq_sys::*;
use std::ffi::{CStr, CString};
use std::ptr;
use std::thread;
use std::time::Duration;

#[derive(Clone)]
pub struct StatusDataContext {
    pub kb_search: *mut PGconn,
    pub base_table: String,
}

pub fn get_status_data(
    ctx: &StatusDataContext,
    path: &str,
    data_str: &mut String,
) -> i32 {
    if path.is_empty() {
        eprintln!("Path cannot be empty or NULL");
        return -1;
    }

    let query_buf = format!("SELECT data FROM {} WHERE path = $1 LIMIT 1", ctx.base_table);
    let c_query = unsafe {
        let c_str = CString::new(query_buf).unwrap();
        let path_cstr = CString::new(path).unwrap();
        let param_values: [*const i8; 1] = [path_cstr.as_ptr()];
        let param_lengths: [path.len() as i32];
        let param_formats: [0i32];
        PQexecParams(
            ctx.kb_search,
            c_str.as_ptr(),
            1,
            ptr::null(),
            param_values.as_ptr(),
            param_lengths.as_ptr(),
            param_formats.as_ptr ptr::null(),
            param_formats.as_ptr(),
            0,
        )
    };

    if unsafe { PQresultStatus(res) } != PGRES_TUPLES_OK {
        eprintln!(
            "Error executing query: {}",
            unsafe { CStr::from_ptr(PQresultErrorMessage(res)) }.to_string_lossy()
        );
        unsafe { PQclear(res) };
        return -1;
    }

    if unsafe { PQntuples(res) } == 0 {
        eprintln!("No data found for path: {}", path);
        unsafe { PQclear(res) };
        return -1;
    }

    let data = unsafe { CStr::from_ptr(PQgetvalue(res, 0, 0)).to_string_lossy().to_string() };
    *data_str = data;

    unsafe { PQclear(res) };
    0
}

pub fn set_status_data(
    ctx: &StatusDataContext,
    path: &str,
    data: &str,
    retry_count: i32,
    retry_delay: f64,
    success: &mut i32,
    message: &mut String,
) -> i32 {
    if path.is_empty() {
        eprintln!("Path cannot be empty or NULL");
        return -1;
    }
    if data.is_empty() {
        eprintln!("Data cannot be empty or NULL");
        return -1;
    }

    if retry_count < 0 {
        eprintln!("Retry count must be non-negative");
        return -1;
    }

    if retry_delay < 0.0 {
        eprintln!("Retry delay must be non-negative");
        return -1;
    }

    let query_buf = format!(
        "INSERT INTO {} (path, data) VALUES ($1, $2) ON CONFLICT (path) DO UPDATE SET data = EXCLUDED.data RETURNING path, (xmax = 0) AS was_inserted",
        ctx.base_table
    );
    let c_query = CString::new(query_buf).unwrap();

    let path_cstr = CString::new(path).unwrap();
    let data_cstr = CString::new(data).unwrap();
    let param_values: [*const i8; 2] = [path_cstr.as_ptr(), data_cstr.as_ptr()];
    let param_lengths: [i32; 2] = [path.len() as i32 i32, data.len() as i32];
    let param_formats: [0i32; 2] = [0, 0];

    let mut last_error: Option<String> = None;
    let mut attempt = 0;
    let mut result = -1;

    'retry: for a in 0..=retry_count {
        attempt = a + 1;

        let begin_str = CString::new("BEGIN").unwrap();
        let begin_res = unsafe { PQexec(ctx.kb_search, begin_str.as_ptr()) };
        if unsafe { PQresultStatus(begin_res) } != PGRES_COMMAND_OK {
            eprintln!(
                "Error starting transaction: {}",
                unsafe { CStr::from_ptr(PQresultErrorMessage(begin_res)).to_string_lossy()
            );
            unsafe { PQclear(begin_res) };
            return -1;
        }
        unsafe { PQclear(begin_res) };

        let res = unsafe {
            PQexecParams(
                ctx.kb_search,
                c_query.as_ptr(),
                2,
                ptr::null(),
                param_values.as_ptr(),
                param_lengths.as_ptr(),
                param_formats.as_ptr(),
                param_formats.as_ptr(),
                0,
            )
        };
        let status = unsafe { PQresultStatus(res) };

        if status == PGRES_TUPLES_OK {
            let n = unsafe { PQntuples(res) };
            if n > 0 {
                let returned_path = unsafe { CStr::from_ptr(PQgetvalue(res, 0, 0)).to_str().unwrap() };
                let was_inserted = unsafe { CStr::from_ptr(PQgetvalue(res, 0, 1)).to_str().unwrap() == "t" };
                let operation = if was_inserted { "inserted" } else { "updated" };

                let commit_str = CString::new("COMMIT").unwrap();
                let commit_res = unsafe { PQexec(ctx.kb_search, commit_str.as_ptr()) };
                if unsafe { PQresultStatus(commit_res) } != PGRES_COMMAND_OK {
                    eprintln!(
                        "Error committing transaction: {}",
                        unsafe { CStr::from_ptr(PQresultErrorMessage(commit_res)) .to_string_lossy() }
                    );
                    unsafe { PQclear(commit_res) };
                    unsafe { PQclear(res) };
                    return -1;
                }
                unsafe { PQclear(commit_res) };

                *success = 1;
                message.clear();
                message.push_str(&format!("Successfully {} data for path: {}", operation, returned_path));

                unsafe { PQclear(res) };
                result = 0;
                break 'retry;
            } else {
                let rollback_str = CString::new("ROLLBACK").unwrap();
                let rollback_res = unsafe { PQexec(ctx.kb_search, rollback_str.as_ptr()) };
                unsafe { PQclear(rollback_res) };
                eprintln!("Database operation completed but no result was returned");
                unsafe { PQclear(res) };
                return -1;
            }
        } else {
            last_error = Some(unsafe { CStr::from_ptr(PQresultErrorMessage(res)).to_string_lossy().to_string() });
            unsafe { PQclear(res) };

            let rollback_str = CString::new("ROLLBACK").unwrap();
            let rollback_res = unsafe { PQexec(ctx.kb_search, rollback_str.as_ptr()) };
            unsafe { PQclear(rollback_res) };

            if a < retry_count {
                thread::sleep(Duration::from_secs_f64(retry_delay));
                continue 'retry;
            } else {
                *success = 0;
                message.clear();
                let err = last_error.as_ref().map(|s| s.as_str()).unwrap_or("Unknown error");
                message.push_str(&format!("Failed to set status data for path '{}' after {} attempts: {}", path, retry_count + 1, err));
                result = -1;
                break;
            }
        }
    }

    result
}
