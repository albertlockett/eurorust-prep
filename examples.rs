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

//! otap-dataflow/crates/pdata-views/src/logs.rs

/// View for LogRecords
pub trait LogRecordView {
    /// The `AttributeView` type associated with this impl of the 
    /// `LogRecordView` trait for accessing this LogRecord's attributes
    type Attribute<'att>: AttributeView
    where
        Self: 'att;

    /// The associated attribute iterator type for this impl of the 
    /// trait. The iterator will yield borrowed references that must
    /// live as long as the lifetime 'att
    type AttributeIter<'att>: Iterator<Item = Self::Attribute<'att>>
    where
        Self: 'att;

    /// The associated `AnyValueView` type associated with this impl
    /// of `LogRecordView` trait for accessing this LogRecord's Body
    type Body<'bod>: AnyValueView<'bod>
    where
        Self: 'bod;

    /// access  the time when the event occurred.
    /// returns `None` if the timestamp is unknown or missing
    fn time_unix_nano(&self) -> Option<u64>;

    /// access the time when the event was observed by the collection 
    /// system. returns `None` if the timestamp is unknown or missing
    fn observed_time_unix_nano(&self) -> Option<u64>;

    // ...
}


/// Implementation of `LogRecordView` backed by protobuf serialized 
/// `LogRecord` message
pub struct RawLogRecord<'a> {
    // contains buf: &[u8] and internal shared parsing state
    bytes_parser: ProtoBytesParser<'a, LogFieldOffsets>,
}

impl LogRecordView for RawLogRecord<'_> {
    // ...
    type AttributeIter<'att> = KeyValueIter<'att, LogFieldOffsets>
    where
        Self: 'att;
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


// example of using arrow-rs SDKs
fn main() {
    use arrow::array::{RecordBatch, StringArray, Int32Array};
    use arrow::compute::kernels::cmp::eq;
    use arrow::compute::or;

    let event_name = StringArray::from_iter_values(["login.success", 
        "request.failed", "file.read_error"]);
    let severity_number = Int32Array::from_iter_values([INFO, WARN, ERROR]);

    let is_important = or(
        &eq(&area_codes, &UInt16Array::new_scalar(WARN)).unwrap(),
        &eq(&area_codes, &UInt16Array::new_scalar(ERROR)).unwrap(),
    );
    println!("{is_important:?}"); // [false, true, true]
}

