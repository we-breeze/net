use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{
        Arc, OnceLock, Weak,
        mpsc::{self, Receiver, RecvTimeoutError, Sender},
    },
    thread,
    time::{Duration, Instant},
};

use crate::{NetError, Result, node::NodePoolInner};

const MAINTENANCE_INTERVAL: Duration = Duration::from_secs(1);
const MAX_BACKGROUND_CONNECTS: usize = 1;

pub(crate) enum Command {
    Register(Weak<NodePoolInner>),
    Finished { id: u64, retry_now: bool },
}

#[derive(Clone)]
struct Maintainer {
    sender: Sender<Command>,
}

static MAINTAINER: OnceLock<std::result::Result<Maintainer, String>> = OnceLock::new();

pub(crate) fn register(node: &Arc<NodePoolInner>) -> Result<()> {
    let maintainer = shared()?;
    maintainer
        .sender
        .send(Command::Register(Arc::downgrade(node)))
        .map_err(|_| NetError::PoolMaintainerStopped)
}

fn shared() -> Result<Maintainer> {
    match MAINTAINER.get_or_init(start) {
        Ok(maintainer) => Ok(maintainer.clone()),
        Err(message) => Err(NetError::InvalidConfig(message.clone())),
    }
}

fn start() -> std::result::Result<Maintainer, String> {
    let (sender, receiver) = mpsc::channel();
    let thread_sender = sender.clone();
    thread::Builder::new()
        .name("brz-net-maintenance".into())
        .spawn(move || run(receiver, thread_sender))
        .map_err(|error| format!("failed to start shared node-pool maintainer: {error}"))?;
    Ok(Maintainer { sender })
}

fn run(receiver: Receiver<Command>, sender: Sender<Command>) {
    let mut nodes = HashMap::<u64, Weak<NodePoolInner>>::new();
    let mut ready = VecDeque::new();
    let mut queued = HashSet::new();
    let mut in_flight = 0_usize;
    let mut next_tick = Instant::now() + MAINTENANCE_INTERVAL;

    loop {
        let wait = next_tick.saturating_duration_since(Instant::now());
        match receiver.recv_timeout(wait) {
            Ok(command) => {
                handle_command(command, &mut nodes, &mut ready, &mut queued, &mut in_flight)
            }
            Err(RecvTimeoutError::Timeout) => {
                scan_nodes(&mut nodes, &mut ready, &mut queued);
                next_tick = Instant::now() + MAINTENANCE_INTERVAL;
            }
            Err(RecvTimeoutError::Disconnected) => return,
        }

        dispatch(&nodes, &mut ready, &mut queued, &mut in_flight, &sender);
    }
}

fn handle_command(
    command: Command,
    nodes: &mut HashMap<u64, Weak<NodePoolInner>>,
    ready: &mut VecDeque<u64>,
    queued: &mut HashSet<u64>,
    in_flight: &mut usize,
) {
    match command {
        Command::Register(node) => {
            let Some(node) = node.upgrade() else {
                return;
            };
            let id = node.maintenance_id();
            nodes.insert(id, Arc::downgrade(&node));
            if node.maintenance_tick() {
                enqueue(id, ready, queued);
            }
        }
        Command::Finished { id, retry_now } => {
            *in_flight = in_flight.saturating_sub(1);
            if retry_now {
                enqueue(id, ready, queued);
            }
        }
    }
}

fn scan_nodes(
    nodes: &mut HashMap<u64, Weak<NodePoolInner>>,
    ready: &mut VecDeque<u64>,
    queued: &mut HashSet<u64>,
) {
    nodes.retain(|id, node| {
        let Some(node) = node.upgrade() else {
            queued.remove(id);
            return false;
        };
        if node.maintenance_tick() {
            enqueue(*id, ready, queued);
        }
        true
    });
}

fn enqueue(id: u64, ready: &mut VecDeque<u64>, queued: &mut HashSet<u64>) {
    if queued.insert(id) {
        ready.push_back(id);
    }
}

fn dispatch(
    nodes: &HashMap<u64, Weak<NodePoolInner>>,
    ready: &mut VecDeque<u64>,
    queued: &mut HashSet<u64>,
    in_flight: &mut usize,
    sender: &Sender<Command>,
) {
    while *in_flight < MAX_BACKGROUND_CONNECTS {
        let Some(id) = ready.pop_front() else {
            return;
        };
        queued.remove(&id);

        let Some(node) = nodes.get(&id).and_then(Weak::upgrade) else {
            continue;
        };
        let Some((endpoints, reservation)) = node.reserve_background_connection(sender.clone())
        else {
            continue;
        };

        *in_flight += 1;
        node.runtime().spawn(reservation.connect(endpoints));
    }
}
