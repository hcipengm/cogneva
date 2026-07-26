//! Windows Service entry point for cogneva.
//! Activate with the `--service` command-line flag:
//! ```powershell
//! sc.exe create cogneva binPath= "C:\ProgramData\cogneva\cogneva.exe --service"
//! sc.exe start cogneva
//! ```

use std::ffi::OsString;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tracing::{error, info};
use windows_service::{
    define_windows_service,
    service::{
        ServiceControl, ServiceControlAccept, ServiceExitCode, ServiceState, ServiceStatus,
        ServiceType,
    },
    service_control_handler::{self, ServiceControlHandlerResult},
    service_dispatcher,
};

const SERVICE_NAME: &str = "cogneva";

static STOP_REQUESTED: AtomicBool = AtomicBool::new(false);

pub fn is_stop_requested() -> bool {
    STOP_REQUESTED.load(Ordering::SeqCst)
}

define_windows_service!(ffi_service_main, service_main);

fn service_main(_args: Vec<OsString>) {
    if let Err(e) = run_service() {
        error!("Windows service error: {}", e);
    }
}

fn run_service() -> windows_service::Result<()> {
    let shutdown_tx = Arc::new(std::sync::Mutex::new(None::<std::sync::mpsc::Sender<()>>));
    let shutdown_tx_clone = shutdown_tx.clone();

    let status_handle = service_control_handler::register(
        SERVICE_NAME,
        move |control_event| -> ServiceControlHandlerResult {
            match control_event {
                ServiceControl::Stop => {
                    info!("Windows Service stop requested");
                    STOP_REQUESTED.store(true, Ordering::SeqCst);
                    if let Some(shutdown) = crate::SHUTDOWN.get() {
                        shutdown.trigger();
                    }
                    if let Ok(lock) = shutdown_tx_clone.lock() {
                        if let Some(tx) = lock.as_ref() {
                            let _ = tx.send(());
                        }
                    }
                    ServiceControlHandlerResult::NoError
                }
                ServiceControl::Interrogate => ServiceControlHandlerResult::NoError,
                _ => ServiceControlHandlerResult::NotImplemented,
            }
        },
    )?;

    let set_status = |state: ServiceState| {
        let status = ServiceStatus {
            service_type: ServiceType::OWN_PROCESS,
            current_state: state,
            controls_accepted: if state == ServiceState::Running {
                ServiceControlAccept::STOP
            } else {
                ServiceControlAccept::empty()
            },
            exit_code: ServiceExitCode::Win32(0),
            checkpoint: 0,
            wait_hint: std::time::Duration::default(),
            process_id: None,
        };
        let _ = status_handle.set_service_status(status);
    };

    set_status(ServiceState::Running);
    info!("Windows Service '{}' running", SERVICE_NAME);

    // Block until the service is stopped
    let (tx, rx) = std::sync::mpsc::channel::<()>();
    {
        let mut lock = shutdown_tx.lock().map_err(|_| {
            windows_service::WinError::new(windows_service::Error::ServiceSpecificError(
                1,
                OsString::from("mutex poisoned"),
            ))
        })?;
        *lock = Some(tx);
    }

    let _ = rx.recv();

    set_status(ServiceState::Stopped);
    info!("Windows Service '{}' stopped", SERVICE_NAME);
    Ok(())
}

/// Entry point when running as a Windows Service.
pub fn run() -> Result<(), Box<dyn std::error::Error>> {
    service_dispatcher::start(SERVICE_NAME, ffi_service_main)?;
    Ok(())
}
