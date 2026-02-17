use std::env;

use postgres::Config;
use r2d2::Pool;
use r2d2_postgres::PostgresConnectionManager;

pub fn load_dotenv() {
    let _ = dotenvy::dotenv();
}

pub fn get_cfg_from_env() -> Config {
    let mut cfg = Config::new();
    cfg.dbname(&env::var("DATABASE_NAME").unwrap())
        .user(&env::var("DBUSER").unwrap())
        .password(env::var("DBPASSWORD").unwrap())
        .host(&env::var("DBHOST").unwrap());
    if let Some(port) = env::var("DBPORT").ok().and_then(|e| e.parse().ok()) {
        cfg.port(port);
    }
    cfg
}

pub fn get_pool_from_env() -> crate::PoolConnection {
    load_dotenv();
    Pool::builder()
        .build(PostgresConnectionManager::new(
            get_cfg_from_env(),
            postgres::NoTls,
        ))
        .unwrap()
}
