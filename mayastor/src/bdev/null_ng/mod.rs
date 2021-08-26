use spdk::{
    cpu_cores::Cores,
    BdevBuilder,
    BdevIo,
    BdevModule,
    BdevModuleBuild,
    BdevModuleInit,
    BdevOps,
    IoChannel,
    IoDevice,
    IoType,
    Poller1,
    PollerBuilder,
};
use std::{cell::RefCell};
use std::rc::Rc;

const NULL_MODULE_NAME: &'static str = "NullNg";

/// TODO
struct NullBdevModule {}

impl BdevModuleInit for NullBdevModule {
    fn module_init() -> i32 {
        NullIoDevice::create("nullng0");
        0
    }
}

impl BdevModuleBuild for NullBdevModule {}

pub fn register() {
    NullBdevModule::builder(NULL_MODULE_NAME)
        .with_module_init()
        .register();
}

//===================== IOChannel ==================================

/// Per-core channel data.
struct NullIoChannelData {
    _poller: Poller1<'static>,
    iovs: Rc<RefCell<Vec<BdevIo<NullIoDevice>>>>,
    my_poller: i64,
    my_data: i64,
}

impl NullIoChannelData {
    fn new(a: i64, b: i64) -> Self {
        let iovs = Rc::new(RefCell::new(Vec::new()));
        let poller_iovs = Rc::clone(&iovs);

        let poller = PollerBuilder::new()
            .with_interval(1000)
            .with_poll_fn(move || {
                let ready: Vec<_> = poller_iovs.borrow_mut().drain(..).collect();
                let cnt = ready.len();
                if cnt > 0 {
                    dbgln!(NullIoDevice, "poller"; ">>>> poll: cnt={}", cnt);
                }
                ready.iter().for_each(|io: &BdevIo<_>| io.ok());
                cnt as i32
            })
            .build();

        let res = Self {
            _poller: poller,
            iovs,
            my_poller: a,
            my_data: b,
        };
        dbgln!(NullIoChannelData, res.dbg(); "new");
        res
    }

    fn dbg(&self) -> String {
        format!("NIO.Dat[p '{}' d '{}']", self.my_poller, self.my_data)
    }
}

impl Drop for NullIoChannelData {
    fn drop(&mut self) {
        dbgln!(NullIoChannelData, self.dbg(); "drop");
    }
}

/// 'Null' I/O device structure.
struct NullIoDevice {
    name: String,
    smth: u64,
    next_chan: RefCell<i64>,
}

/// TODO
impl Drop for NullIoDevice {
    fn drop(&mut self) {
        dbgln!(NullIoDevice, self.dbg(); "drop");
    }
}

/// TODO
impl IoDevice for NullIoDevice {
    type ChannelData = NullIoChannelData;

    /// TODO
    fn io_channel_create(&self) -> NullIoChannelData {
        dbgln!(NullIoDevice, self.dbg(); "io_channel_create");

        let mut x = self.next_chan.borrow_mut();
        *x += 1;
        self.get_io_device_id();

        NullIoChannelData::new(123, *x)
    }

    /// TODO
    fn io_channel_destroy(&self, io_chan: NullIoChannelData) {
        dbgln!(NullIoDevice, self.dbg(); "io_channel_destroy: <{}>", io_chan.dbg());
    }
}

/// TODO
impl BdevOps for NullIoDevice {
    type ChannelData = NullIoChannelData;

    /// TODO
    fn destruct(self: Box<Self>) {
        dbgln!(NullIoDevice, self.dbg(); "destruct");
        self.io_device_unregister();
    }

    fn submit_request(
        &self,
        io_chan: IoChannel<NullIoChannelData>,
        bio: BdevIo<NullIoDevice>,
    ) {
        let chan_data = io_chan.channel_data();

        dbgln!(NullIoDevice, self.dbg();
            "submit req: my cd {}", chan_data.dbg());

        match bio.io_type() {
            IoType::Read | IoType::Write => {
                dbgln!(NullIoDevice, self.dbg(); ">>>> push BIO");
                chan_data.iovs.borrow_mut().push(bio)
            },
            _ => bio.fail(),
        };
    }

    /// TODO
    fn io_type_supported(&self, io_type: IoType) -> bool {
        dbgln!(NullIoDevice, self.dbg(); "io_type_supported?: {:?}", io_type);
        matches!(io_type, IoType::Read | IoType::Write)
    }
}

/// TODO
impl NullIoDevice {
    fn create(name: &str) {
        let bm = BdevModule::find_by_name(NULL_MODULE_NAME).unwrap();

        let io_dev = Box::new(NullIoDevice {
            name: String::from(name),
            smth: 789,
            next_chan: RefCell::new(10),
        });

        let bdev = BdevBuilder::new()
            .with_context(&io_dev)
            .with_module(&bm)
            .with_name(name)
            .with_product_name("nullblk thing")
            .with_block_length(1 << 12)
            .with_block_count(1 << 20)
            .with_required_alignment(12)
            .build();

        io_dev.io_device_register(name);
        bdev.bdev_register();

        dbgln!(NullIoDevice, ""; "created '{}'", name);
    }

    fn dbg(&self) -> String {
        format!(
            "NIO.Dev[id '{:p}' name '{}' smth '{}']",
            self.get_io_device_id(),
            self.name,
            self.smth
        )
    }
}
