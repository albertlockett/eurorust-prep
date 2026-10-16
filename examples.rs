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

/// View trait for LogRecords:
pub trait LogRecordView {
    // access key-value attributes:
    type Attribute<'att>: AttributeView where Self: 'att>
    type AttributeIter<'att>: Iterator<Item = Self::Attribute<'att>> 
        where Self: 'att;  
    fn attributes(&self) -> Self::AttributeIter<'_>;

    // other primitive fields:
    fn time_unix_nano(&self) -> Option<u64>;
    fn severity_number(&self) -> Option<i32>;

    // ...
}

// Proto backed implementation
pub struct RawLogRecord<'a> { buf: &'a [u8] },

impl LogRecordView for RawLogRecord<'_> {
    type Attribute<'att> = RawKeyValue<'att> where Self: 'att;
    // ...
}
pub struct RawKeyValue<'a> { buf: &'a [u8] }

impl AttributeView for RawKeyValue<'_> { /* ... */ }





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
        // ..
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
        // ...
    }
}


// example of using arrow-rs SDKs
fn main() {
    use arrow::array::*;
    use arrow::compute::kernels::cmp::eq;
    use arrow::compute::or;

    // Arrow "Arrays" are columns of some data type
    let event_name = StringArray::from_iter_values([
        "login.success", 
        "request.failed", 
        "file.read_error",
    ]);
    let severity_number = Int32Array::from_iter_values([INFO, WARN, ERROR]);

    // RecordBatch = collection columns with schema
    let batch = RecordBatch::try_from_iter([
        ("event_name", make_array(event_name.to_data())),
        ("severity_text", make_array(severity_number.to_data())),
    ])?;

    // compute kernels for transforming arrow data & batches
    let is_important: BooleanArray = or(
        &eq(&severity_number, &Int32Array::new_scalar(WARN)).unwrap(),
        &eq(&severity_number, &Int32Array::new_scalar(ERROR)).unwrap(),
    )?;
    println!("{is_important:?}"); // [false, true, true]
}

