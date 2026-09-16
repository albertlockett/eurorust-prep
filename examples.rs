//-------- SLIDE 7

//! otap-dataflow/crates/channel/src/mpsc.rs

/// A sender for the channel.
pub struct Sender<T> {
    channel: Rc<Channel<T>>,
}

/// A receiver for the channel.
pub struct Receiver<T> {
    channel: Rc<Channel<T>>,
}

/// A single-threaded MPSC channel.
pub struct Channel<T> {
    state: RefCell<ChannelState<T>>,
}

struct ChannelState<T> {
    buffer: VecDeque<T>,
    capacity: usize,
    is_closed: bool,
    senders: usize,
    has_receiver: bool,
    receiver_waker: Option<Waker>,
    // ...
}





//! otap-dataflow/crates/contrib-nodes/receivers/kafka_receiver/mod.rs

#[async_trait(?Send)]
impl Receiver<OtapPdata> for KafkaReceiver {
    async fn start(
        mut self: Box<Self>,
        ctrl_msg_recv: ControlChannel<OtapPdata>,
        effect_handler: EffectHandler<OtapPdata>,
    ) -> Result<TerminalState, EngineError> {
        // ...
 
    }
}

//! otap-dataflow/crates/core-nodes/processors/transform_processor/mod.rs

#[async_trait(?Send)]
impl Processor<OtapPdata> for TransformProcessor {
    async fn process(
        &mut self,
        message: Message<OtapPdata>,
        effect_handler: &mut EffectHandler<OtapPdata>,
    ) -> Result<(), EngineError> {
        match message {
            Message::Control(control_msg) => { /* ... */ },
            Message::Pdata(pdata) => { /* ... */}
        }

        ///
    }
}

//! otap-dataflow/crates/core-nodes/exporters/parquet_exporter/mod.rs

#[async_trait(?Send)]
impl Exporter<OtapPdata> for ParquetExporter {
    async fn start(
        mut self: Box<Self>,
        mut msg_chan: ExporterInbox<OtapPdata>,
        effect_handler: EffectHandler<OtapPdata>,
    ) -> Result<TerminalState, Error> {
 
 
    }
}