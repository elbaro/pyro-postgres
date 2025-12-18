use pyo3::prelude::*;

use crate::error::{Error, PyroResult};

/// Connection options for PostgreSQL connections.
///
/// This class provides a builder API for configuring PostgreSQL connection parameters.
/// Methods can be chained to configure multiple options, and the instance can be
/// passed directly to connection methods.
///
/// # Examples
/// ```python
/// # Create from URL
/// opts = Opts("postgres://user:pass@localhost:5432/mydb")
///
/// # Or build manually
/// opts = Opts().host("localhost").port(5432).user("postgres").password("secret").db("mydb")
/// ```
#[pyclass(module = "pyro_postgres", name = "Opts")]
#[derive(Clone, Debug, Default)]
pub struct Opts {
    pub inner: zero_postgres::Opts,
}

#[pymethods]
impl Opts {
    /// Create a new Opts instance.
    ///
    /// # Arguments
    /// * `url` - Optional PostgreSQL connection URL. If provided, parses the URL.
    ///           If not provided, creates default opts.
    ///
    /// # URL Format
    /// ```text
    /// postgres://[username[:password]@]host[:port][/database][?param=value&...]
    /// ```
    ///
    /// # Examples
    /// ```python
    /// # Create default opts
    /// opts = Opts()
    ///
    /// # Create from URL
    /// opts = Opts("postgres://postgres:password@localhost:5432/mydb")
    /// ```
    #[new]
    #[pyo3(signature = (url=None))]
    fn new(url: Option<&str>) -> PyroResult<Self> {
        if let Some(url) = url {
            let inner: zero_postgres::Opts = url.try_into()?;
            Ok(Self { inner })
        } else {
            Ok(Self::default())
        }
    }

    /// Set the hostname or IP address.
    ///
    /// # Arguments
    /// * `hostname` - The hostname or IP address to connect to
    fn host(mut self_: PyRefMut<Self>, hostname: String) -> PyRefMut<Self> {
        self_.inner.host = hostname;
        self_
    }

    /// Set the TCP port number.
    ///
    /// # Arguments
    /// * `port` - The port number (default: 5432)
    fn port(mut self_: PyRefMut<Self>, port: u16) -> PyRefMut<Self> {
        self_.inner.port = port;
        self_
    }

    /// Set the Unix socket path for local connections.
    ///
    /// # Arguments
    /// * `path` - The path to the Unix socket file
    fn socket(mut self_: PyRefMut<Self>, path: Option<String>) -> PyRefMut<Self> {
        self_.inner.socket = path;
        self_
    }

    /// Set the username for authentication.
    ///
    /// # Arguments
    /// * `username` - The username
    fn user(mut self_: PyRefMut<Self>, username: String) -> PyRefMut<Self> {
        self_.inner.user = username;
        self_
    }

    /// Set the password for authentication.
    ///
    /// # Arguments
    /// * `password` - The password
    fn password(mut self_: PyRefMut<Self>, password: Option<String>) -> PyRefMut<Self> {
        self_.inner.password = password;
        self_
    }

    /// Set the database name to connect to.
    ///
    /// # Arguments
    /// * `database` - The database name
    fn db(mut self_: PyRefMut<Self>, database: Option<String>) -> PyRefMut<Self> {
        self_.inner.database = database;
        self_
    }

    /// Set the application name to report to the server.
    ///
    /// # Arguments
    /// * `name` - The application name
    fn application_name(mut self_: PyRefMut<Self>, name: Option<String>) -> PyRefMut<Self> {
        self_.inner.application_name = name;
        self_
    }

    /// Set the SSL mode for the connection.
    ///
    /// # Arguments
    /// * `mode` - One of: "disable", "prefer", "require"
    fn ssl_mode(mut self_: PyRefMut<'_, Self>, mode: String) -> PyroResult<PyRefMut<'_, Self>> {
        self_.inner.ssl_mode = match mode.as_str() {
            "disable" => zero_postgres::SslMode::Disable,
            "prefer" => zero_postgres::SslMode::Prefer,
            "require" => zero_postgres::SslMode::Require,
            _ => {
                return Err(Error::IncorrectApiUsageError(
                    "Invalid ssl_mode. Use: disable, prefer, require",
                ))
            }
        };
        Ok(self_)
    }

    /// Enable or disable automatic upgrade from TCP to Unix socket.
    ///
    /// When enabled and connected via TCP to loopback, the driver will query
    /// unix_socket_directories and reconnect using the Unix socket for better performance.
    ///
    /// # Arguments
    /// * `enable` - Whether to enable upgrade to Unix socket (default: true)
    fn prefer_unix_socket(mut self_: PyRefMut<Self>, enable: bool) -> PyRefMut<Self> {
        self_.inner.prefer_unix_socket = enable;
        self_
    }

    /// Set the maximum number of idle connections in the pool.
    ///
    /// # Arguments
    /// * `count` - Maximum idle connections (default: 100)
    fn pool_max_idle_conn(mut self_: PyRefMut<Self>, count: usize) -> PyRefMut<Self> {
        self_.inner.pool_max_idle_conn = count;
        self_
    }

    /// Set the maximum number of concurrent connections (active + idle).
    ///
    /// # Arguments
    /// * `count` - Maximum concurrent connections, or None for unlimited (default: None)
    fn pool_max_concurrency(mut self_: PyRefMut<Self>, count: Option<usize>) -> PyRefMut<Self> {
        self_.inner.pool_max_concurrency = count;
        self_
    }

    fn __repr__(&self) -> String {
        format!("{:?}", self.inner)
    }
}

/// Helper to convert either a String URL or Opts object to zero_postgres::Opts
pub fn resolve_opts(_py: Python<'_>, url_or_opts: &Bound<'_, PyAny>) -> PyroResult<zero_postgres::Opts> {
    // Try to extract as string first
    if let Ok(url) = url_or_opts.extract::<String>() {
        let inner: zero_postgres::Opts = url.as_str().try_into()?;
        return Ok(inner);
    }

    // Try to extract as Opts
    if let Ok(opts) = url_or_opts.extract::<Opts>() {
        return Ok(opts.inner);
    }

    // Try to cast as Opts pyclass
    if let Ok(opts_ref) = url_or_opts.cast::<Opts>() {
        return Ok(opts_ref.borrow().inner.clone());
    }

    Err(Error::IncorrectApiUsageError(
        "Expected a connection URL string or Opts object",
    ))
}
