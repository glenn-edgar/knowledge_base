
use pq_sys::*;
use std::ffi::{CStr, CString};
use std::ptr;

pub fn create_pg_connection(
    dbname: Option<&str>,
    user: Option<&str>,
    password: Option<&str>,
    host: Option<&str>,
    port: Option<&str>,
) -> *mut PGconn {
    let conninfo = format!(
        "dbname={} user={} password={} host={} port={}",
        dbname.unwrap_or(""),
        user.unwrap_or(""),
        password.unwrap_or(""),
        host.unwrap_or("localhost"),
        port.unwrap_or("5432")
    );

    let c_conninfo = match CString::new(conninfo) {
        Ok(s) => s,
        Err(_) => return ptr::null_mut(),
    };

    let conn = unsafe { PQconnectdb(c_conninfo.as_ptr()) };

    if unsafe { PQstatus(conn) } != ConnStatusType::CONNECTION_OK {
        let err_msg = unsafe { CStr::from_ptr(PQerrorMessage(conn)) };
        eprintln!("Connection failed: {}", err_msg.to_string_lossy());
        unsafe { PQfinish(conn) };
        return ptr::null_mut();
    }

    conn
}
