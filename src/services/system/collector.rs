use crate::google::datavalue::{Datarow, Datavalue};
use crate::notifications::{Notification, Sender};
use chrono::{DateTime, NaiveDateTime};
use std::path::Path;
use std::thread;
use std::time::Duration;
use sysinfo::{Networks, Pid, Process as SysinfoProcess, ProcessesToUpdate, System, Uid, Users};
use tracing::Level;

pub const BASIC_LOG: &str = "basic";
pub const MEMORY_USE: &str = "memory_use";
pub const SWAP_USE: &str = "swap_use";
pub const DISK_USE: &str = "disk_use";
pub const CPU: &str = "cpu";

#[cfg(target_os = "linux")]
fn open_files(pid: Pid) -> Option<usize> {
    let dir = format!("/proc/{pid}/fd");
    let path = Path::new(&dir);
    Some(path.read_dir().ok()?.count())
}

#[cfg(not(target_os = "linux"))]
fn open_files(_: Pid) -> Option<usize> {
    None
}

#[derive(Debug, Clone)]
struct ProcessInfo {
    pid: Pid,
    name: String,
    user_id: Option<Uid>,
    effective_user_id: Option<Uid>,
    cpu_percent: f32,
    memory_used: u64,
    virtual_memory: u64,
    memory_use: f32,
    disk_read: u64,
    disk_write: u64,
    start_time: NaiveDateTime,
    open_files: Option<usize>, // only for Linux and not for all processes
    is_thread: bool,           // only for Linux
}

impl ProcessInfo {
    #[allow(clippy::cast_precision_loss)]
    fn from(sysinfo_process: &SysinfoProcess, total_memory: u64) -> Self {
        let name = sysinfo_process
            .exe()
            .and_then(|name| name.file_name())
            .map(|filename| filename.to_string_lossy().into_owned())
            .or(sysinfo_process.cmd().first().and_then(|cmd| {
                Path::new(&cmd)
                    .file_name()
                    .map(|filename| filename.to_string_lossy().into_owned())
            }))
            .unwrap_or_else(|| sysinfo_process.name().to_string_lossy().into_owned());

        // SAFE: memory use should be under 100.0
        // roundings errors are acceptable here
        let memory_use: f32 = (100 * sysinfo_process.memory() / total_memory) as f32;

        Self {
            pid: sysinfo_process.pid(),
            name,
            user_id: sysinfo_process.user_id().cloned(),
            effective_user_id: sysinfo_process.effective_user_id().cloned(),
            cpu_percent: sysinfo_process.cpu_usage(),
            memory_used: sysinfo_process.memory(),
            virtual_memory: sysinfo_process.virtual_memory(),

            memory_use,
            disk_read: sysinfo_process.disk_usage().read_bytes,
            disk_write: sysinfo_process.disk_usage().written_bytes,
            start_time: DateTime::from_timestamp(
                sysinfo_process
                    .start_time()
                    .try_into()
                    .expect("assert: it is possible to build datetime from process start time"),
                0,
            )
            .expect("assert: process start_time timestamp should be valid")
            .naive_local(),
            open_files: open_files(sysinfo_process.pid()),
            is_thread: sysinfo_process.thread_kind().is_some(),
        }
    }
}

#[allow(clippy::cast_possible_truncation)]
#[allow(clippy::cast_sign_loss)]
fn top_cpu_process(processes: &mut [ProcessInfo]) -> &ProcessInfo {
    // SAFE cast from f32 to u32 just for sorting purposes
    processes.sort_unstable_by_key(|p| (p.cpu_percent * 100.0) as u32);
    processes
        .last()
        .expect("assert: processes list should contain at least one process")
}

fn top_disk_read_process(processes: &mut [ProcessInfo]) -> &ProcessInfo {
    processes.sort_unstable_by_key(|p| p.disk_read);
    processes
        .last()
        .expect("assert: processes list should contain at least one process")
}

fn top_disk_write_process(processes: &mut [ProcessInfo]) -> &ProcessInfo {
    processes.sort_unstable_by_key(|p| p.disk_read);
    processes
        .last()
        .expect("assert: processes list should contain at least one process")
}

fn top_memory_process(processes: &mut [ProcessInfo]) -> &ProcessInfo {
    processes.sort_unstable_by_key(|p| p.memory_used);
    processes
        .last()
        .expect("assert: processes list should contain at least one process")
}

fn top_open_files_process(processes: &mut [ProcessInfo]) -> Option<&ProcessInfo> {
    processes.sort_unstable_by_key(|p| p.open_files);
    processes.last()
}

fn processes_then_threads(processes: &mut [ProcessInfo]) {
    processes.sort_unstable_by_key(|p| (p.is_thread, p.open_files.is_none()));
}

fn process_to_values(process: &ProcessInfo, users: &Users) -> Vec<(String, Datavalue)> {
    let user = process
        .user_id
        .as_ref()
        .and_then(|uid| users.get_user_by_id(uid))
        .map(|u| Datavalue::Text(u.name().to_string()))
        .unwrap_or(Datavalue::NotAvailable);
    let effective_user = process
        .effective_user_id
        .as_ref()
        .and_then(|uid| users.get_user_by_id(uid))
        .map(|u| Datavalue::Text(u.name().to_string()))
        .unwrap_or(Datavalue::NotAvailable);
    let open_files = process
        .open_files
        .map(|open_files| {
            Datavalue::Integer(
                u32::try_from(open_files).expect("assert: number of opened files fits u32"),
            )
        })
        .unwrap_or(Datavalue::NotAvailable);

    vec![
        (
            "pid".to_string(),
            Datavalue::IntegerID(process.pid.as_u32()),
        ),
        (
            "name".to_string(),
            Datavalue::Text(process.name.to_string()),
        ),
        ("user".to_string(), user),
        ("effective_user".to_string(), effective_user),
        (
            "start_time".to_string(),
            Datavalue::Datetime(process.start_time),
        ),
        (
            "virtual_memory".to_string(),
            Datavalue::Size(process.virtual_memory),
        ),
        (
            "memory_used".to_string(),
            Datavalue::Size(process.memory_used),
        ),
        (
            MEMORY_USE.to_string(),
            // SAFE casting percentage from f32 to f64
            Datavalue::HeatmapPercent(f64::from(process.memory_use)),
        ),
        (
            CPU.to_string(),
            // SAFE casting percentage from f32 to f64
            Datavalue::HeatmapPercent(f64::from(process.cpu_percent)),
        ),
        ("disk_read".to_string(), Datavalue::Size(process.disk_read)),
        (
            "disk_write".to_string(),
            Datavalue::Size(process.disk_write),
        ),
        ("open_files".to_string(), open_files),
    ]
}

pub(super) fn initialize() -> System {
    sysinfo::set_open_files_limit(0);
    let mut sys = System::new();
    sys.refresh_memory();
    sys.refresh_processes(ProcessesToUpdate::All, true);
    sys
}

pub struct SystemInfo {
    pub name: Option<String>,
    pub long_os_version: Option<String>,
    pub kernel_version: Option<String>,
    pub host_name: Option<String>,
    pub total_memory: u64,
    pub cpu_arch: String,
    pub boot_time: u64,
}

pub fn system_info() -> SystemInfo {
    let sys = initialize();
    SystemInfo {
        name: System::name(),
        long_os_version: System::long_os_version(),
        kernel_version: System::kernel_version(),
        host_name: System::host_name(),
        total_memory: sys.total_memory(),
        cpu_arch: System::cpu_arch(),
        boot_time: System::boot_time(),
    }
}

#[allow(clippy::cast_precision_loss)]
pub(super) fn collect(
    sys: &mut System,
    mounts: &[String],
    names: &[String],
    scrape_time: NaiveDateTime,
    messenger: &Sender,
) -> Result<Vec<Datarow>, String> {
    sys.refresh_all();
    thread::sleep(Duration::from_secs(1));
    sys.refresh_all();
    let users = Users::new_with_refreshed_list();
    let sysinfo_processes = sys.processes();
    let total_memory = sys.total_memory();
    let mut processes_infos = Vec::with_capacity(sysinfo_processes.len());
    for (_, p) in sysinfo_processes.iter() {
        processes_infos.push(ProcessInfo::from(p, total_memory));
    }

    let boot_time = DateTime::from_timestamp(
        System::boot_time()
            .try_into()
            .expect("assert: it is possible to build datetime from system boot time"),
        0,
    )
    .expect("assert: system boot time timestamp should be valid")
    .naive_local();
    let basic = [
        (
            MEMORY_USE.to_string(),
            // SAFE for percentage calculation to cast from u64 to f64
            Datavalue::HeatmapPercent(100.0 * sys.used_memory() as f64 / total_memory as f64),
        ),
        (
            SWAP_USE.to_string(),
            // SAFE for percentage calculation to cast from u64 to f64
            Datavalue::HeatmapPercent(100.0 * sys.used_swap() as f64 / sys.total_swap() as f64),
        ),
        ("boot_time".to_string(), Datavalue::Datetime(boot_time)),
        (
            "memory_available".to_string(),
            Datavalue::Size(sys.available_memory()),
        ),
        (
            "swap_available".to_string(),
            Datavalue::Size(sys.free_swap()),
        ),
        (
            "num_of_processes".to_string(),
            Datavalue::Integer(
                u32::try_from(sysinfo_processes.len())
                    .expect("assert: number of system processes fits u32"),
            ),
        ),
    ];
    let cpus = sys.cpus().iter().enumerate().map(|(i, c)| {
        (
            format!("cpu{i}"),
            // SAFE casting percentage from f32 to f64
            Datavalue::HeatmapPercent(f64::from(c.cpu_usage())),
        )
    });

    let basic_values: Vec<(String, Datavalue)> = cpus.into_iter().chain(basic).collect();

    // 1 for basic, 5 for top_ stats, 1 for network
    let mut datarows = Vec::with_capacity(1 + mounts.len() + 5 + names.len() + 1);
    datarows.push(Datarow::new(
        BASIC_LOG.to_string(),
        scrape_time,
        basic_values,
    ));

    let mut disk_stat = disk_stat(sys, mounts, scrape_time, messenger);
    datarows.append(&mut disk_stat);

    let top_cpu = top_cpu_process(&mut processes_infos);
    datarows.push(Datarow::new(
        "top_cpu".to_string(),
        scrape_time,
        process_to_values(top_cpu, &users),
    ));
    let top_memory = top_memory_process(&mut processes_infos);
    datarows.push(Datarow::new(
        "top_memory".to_string(),
        scrape_time,
        process_to_values(top_memory, &users),
    ));
    let top_read = top_disk_read_process(&mut processes_infos);
    datarows.push(Datarow::new(
        "top_disk_read".to_string(),
        scrape_time,
        process_to_values(top_read, &users),
    ));
    let top_write = top_disk_write_process(&mut processes_infos);
    datarows.push(Datarow::new(
        "top_disk_write".to_string(),
        scrape_time,
        process_to_values(top_write, &users),
    ));
    if let Some(top_open_files) = top_open_files_process(&mut processes_infos) {
        datarows.push(Datarow::new(
            "top_open_files".to_string(),
            scrape_time,
            process_to_values(top_open_files, &users),
        ));
    }

    processes_then_threads(&mut processes_infos);
    for name in names {
        if let Some(p) = processes_infos.iter().find(|p| p.name.contains(name)) {
            datarows.push(Datarow::new(
                name.clone(),
                scrape_time,
                process_to_values(p, &users),
            ));
        } else {
            let message = format!(
                "process containing `{name}` in its name is not found to collect process statistics"
            );
            tracing::warn!("{}", message);
            messenger.send_nonblock(Notification::new(message, Level::WARN));
        }
    }

    let networks = Networks::new_with_refreshed_list();
    let network_values: Vec<(String, Datavalue)> = networks
        .into_iter()
        .flat_map(|(interface_name, data)| {
            [
                (
                    format!("{interface_name}_total_received"),
                    Datavalue::Size(data.total_received()),
                ),
                (
                    format!("{interface_name}_new_received"),
                    Datavalue::Size(data.received()),
                ),
                (
                    format!("{interface_name}_total_transmitted"),
                    Datavalue::Size(data.total_transmitted()),
                ),
                (
                    format!("{interface_name}_new_transmitted"),
                    Datavalue::Size(data.transmitted()),
                ),
            ]
        })
        .collect();
    datarows.push(Datarow::new(
        "network".to_string(),
        scrape_time,
        network_values,
    ));
    Ok(datarows)
}

#[cfg(target_os = "linux")]
fn disk_stat(
    _: &mut System,
    mounts: &[String],
    scrape_time: NaiveDateTime,
    messenger: &Sender,
) -> Vec<Datarow> {
    let mut datarows = Vec::with_capacity(mounts.len());
    for mount in mounts {
        let stat = match psutil::disk::disk_usage(mount) {
            Ok(s) => s,
            Err(e) => {
                let message =
                    format!("mount `{mount}` is not found to collect disk statistics: `{e}`");
                tracing::warn!("{}", message);
                messenger.send_nonblock(Notification::new(message, Level::WARN));
                continue;
            }
        };
        datarows.push(Datarow::new(
            mount.clone(),
            scrape_time,
            vec![
                (
                    DISK_USE.to_string(),
                    // SAFE casting percentage from f32 to f64
                    Datavalue::HeatmapPercent(f64::from(stat.percent())),
                ),
                ("disk_free".to_string(), Datavalue::Size(stat.free())),
            ],
        ));
    }
    datarows
}

#[cfg(not(target_os = "linux"))]
fn disk_stat(
    _sys: &mut System,
    mounts: &[String],
    scrape_time: NaiveDateTime,
    messenger: &Sender,
) -> Vec<Datarow> {
    let mut datarows = Vec::with_capacity(mounts.len());
    let disks = sysinfo::Disks::new_with_refreshed_list();
    let mut mounts_stat: std::collections::HashMap<String, (u64, u64)> = disks
        .iter()
        .map(|d| {
            (
                d.mount_point().to_string_lossy().into_owned(),
                (d.available_space(), d.total_space()),
            )
        })
        .collect();
    for mount in mounts {
        let (available, total) = match mounts_stat.remove(mount) {
            Some(stat) => stat,
            None => {
                let message = format!("mount `{mount}` is not found to collect disk statistics");
                tracing::warn!("{}", message);
                messenger.send_nonblock(Notification::new(message, Level::WARN));
                continue;
            }
        };
        datarows.push(Datarow::new(
            mount.clone(),
            scrape_time,
            vec![
                (
                    DISK_USE.to_string(),
                    // SAFE for percentage calculation to cast from u64 to f64
                    Datavalue::HeatmapPercent(100.0 * (total - available) as f64 / total as f64),
                ),
                ("disk_free".to_string(), Datavalue::Size(available)),
            ],
        ));
    }
    datarows
}
