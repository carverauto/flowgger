//! A Flowgger output that publishes every log record to a NATS JetStream subject.
//! Enable with `--features nats-output`.

#[cfg(feature = "nats-output")]
use {
    super::Output,
    crate::flowgger::{config::Config, merger::Merger},
    async_nats::{jetstream, Client},
    async_nats::jetstream::{stream::StorageType, context::PublishAckFuture},
    std::{
        path::PathBuf,
        sync::{Arc, Mutex, mpsc::Receiver},
        thread,
        time::Duration,
    },
    tokio::{runtime::Builder as RtBuilder, time::timeout},
};

#[cfg(feature = "nats-output")]
pub struct NatsOutput {
    cfg: NatsConfig,
    workers: u32,
}

#[cfg(feature = "nats-output")]
#[derive(Clone)]
struct NatsConfig {
    url:      String,
    subject:  String,
    stream:   String,
    timeout:  Duration,
    tls_cert: Option<PathBuf>,
    tls_key:  Option<PathBuf>,
    tls_ca:   Option<PathBuf>,
}

#[cfg(feature = "nats-output")]
impl NatsOutput {
    pub fn new(cfg: &Config) -> Self {
        // ---- mandatory ----
        let url     = cfg.lookup("output.nats_url")
            .expect("output.nats_url is required")
            .as_str().unwrap().to_owned();
        let subject = cfg.lookup("output.nats_subject")
            .unwrap_or_else(|| panic!("output.nats_subject is required"))
            .as_str().unwrap().to_owned();

        // ---- optional w/ sane defaults ----
        let stream  = cfg.lookup("output.nats_stream")
            .map_or("FLOWGGER".into(), |v| v.as_str().unwrap().to_owned());
        let timeout = Duration::from_millis(
            cfg.lookup("output.nats_timeout")
                .map_or(30_000, |v| v.as_integer().unwrap() as u64));

        let tls_cert = cfg.lookup("output.nats_tls_cert")
            .and_then(|v| Some(PathBuf::from(v.as_str().unwrap())));
        let tls_key  = cfg.lookup("output.nats_tls_key")
            .and_then(|v| Some(PathBuf::from(v.as_str().unwrap())));
        let tls_ca   = cfg.lookup("output.nats_tls_ca_file")
            .and_then(|v| Some(PathBuf::from(v.as_str().unwrap())));

        let workers  = cfg.lookup("output.nats_threads")
            .map_or(1, |v| v.as_integer().unwrap() as u32);

        Self {
            cfg: NatsConfig { url, subject, stream, timeout, tls_cert, tls_key, tls_ca },
            workers,
        }
    }
}

#[cfg(feature = "nats-output")]
struct NatsWorker {
    arx: Arc<Mutex<Receiver<Vec<u8>>>>,
    cfg: NatsConfig,
    merger: Option<Box<dyn Merger + Send>>,
}

#[cfg(feature = "nats-output")]
impl NatsWorker {
    async fn connect(&self) -> Result<(Client, jetstream::Context), async_nats::Error> {
        // NB: async-nats automatically negotiates TLS if the URI is nats**s**://
        // If users need client-cert auth they can pass the custom ConnectOptions here.
        let client = async_nats::connect(self.cfg.url.clone()).await?;
        let js     = jetstream::new(client.clone());

        // Idempotent stream creation (noop if it already exists).
        let _ = js.create_stream(jetstream::stream::Config {
            name:     self.cfg.stream.clone(),
            subjects: vec![self.cfg.subject.clone()],
            storage:  StorageType::File,
            ..Default::default()
        }).await.or_else(|e| {
            let _ = js.create_stream(stream_config.clone()).await;
            // if e.kind() == jetstream::stream::ErrorKind::AlreadyExists { Ok(()) } else { Err(e) }
        })?;

        Ok((client, js))
    }

    async fn run(mut self) {
        let (_, js) = self.connect().await.expect("NATS connection failed");

        loop {
            // Pull a record from Flowgger’s queue synchronously.
            let mut bytes = match { self.arx.lock().unwrap().recv() } {
                Ok(b) => b,
                Err(_) => return, // channel closed – shut the worker down
            };

            if let Some(m) = &self.merger { m.frame(&mut bytes); }

            // Fire-and-wait-for-ack with timeout so we can log failures.
            let ack: PublishAckFuture = js.publish(&self.cfg.subject, bytes.into()).await
                .expect("publish failed");
            if timeout(self.cfg.timeout, ack).await.is_err() {
                eprintln!("NATS ack timed-out after {:?}", self.cfg.timeout);
            }
        }
    }
}

#[cfg(feature = "nats-output")]
impl Output for NatsOutput {
    fn start(&self,
             arx: Arc<Mutex<Receiver<Vec<u8>>>>,
             merger: Option<Box<dyn Merger>>) {

        for _ in 0..self.workers {
            let arx     = Arc::clone(&arx);
            let cfg     = self.cfg.clone();
            let merger  = merger.as_ref().map(|m| m.clone_boxed());

            thread::spawn(move || {
                let rt = RtBuilder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("tokio runtime");

                rt.block_on(async { NatsWorker { arx, cfg, merger }.run().await });
            });
        }
    }
}
