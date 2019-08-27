# Adding a new trace format + mapping

(note that currently these two activities are tied together in the implementation, they could be effectivly seperated but each mapping would need to define a standard input format that it expects and trace format deserialisation would then convert to that standard input.)

## 1. Define a deserialisation format

Define a rust type that will sucessfully parse the trace format by using the Serde deserialisation system. In specific the type should implement the `DeserializeOwned` trait.

### Example
```rust
#[derive(Deserialize)]
struct Event {
    id: Uuid,
    action: String,
    src: Uuid,
    dst: Uuid,
    proc: String,
    path: String,
    trace_offset: Option<u64>,
}
```

## 2. Define the ConcreteTypes and ContextType

Think about the semantics and define any concrete types that are required to express the events that occur. You will also need to define a ContextType that maps the contextual information that will be provided with events.

### Example
```rust
lazy_static!{
    static ref PROC: ConcreteType = ConcreteType {
        pvm_ty: Actor,
        name: "proc",
        props: hashmap!("name" => true),
    };
    static ref FILE: ConcreteType = ConcreteType {
        pvm_ty: Store,
        name: "file",
        props: hashmap!(),
    };

    static ref CTX: ContextType = ContextType {
        name: "context",
        props: vec!["event_id", "trace_offset"],
    };
}
```

## 3. Define the mapping

Define the actual mapping implementation for the trace format. It is often convenient to implement this as a set of functions on the type we defined in (1), with a simple function per event/action type represented in the underlying trace.

### Example
```rust
impl Event {
    fn map_read(&self, pvm: &mut PVMTransaction) -> PVMResult<()> {
        let file = pvm.define(&FILE, self.src, None)?;
        let proc = pvm.define(&PROC, self.dst, None)?;

        pvm.meta(proc, "name", self.proc)?;
        pvm.name(file, Name::Path(self.path))?;

        pvm.source(file, proc)?;

        Ok(())
    }

    fn map_write(&self, pvm: &mut PVMTransaction) -> PVMResult<()> {
        let proc = pvm.define(&PROC, self.src, None)?;
        let file = pvm.define(&FILE, self.dst, None)?;

        pvm.meta(proc, "name", self.proc)?;
        pvm.name(file, Name::Path(self.path))?;

        pvm.sink(proc, file)?;

        Ok(())
    }
}
```

## 4. Implement the Mapped trait

Implement the Mapped trait for the type defined in (1).

### 4.1 Register concrete types in `init()`

The `init()` function should register any ConcreteType or ContextType definitions with the PVM object.

#### Example
```rust
impl Mapped for Event {
    fn init(pvm: &mut PVM) {
        pvm.register_concrete_type(&PROC);
        pvm.register_concrete_type(&FILE);
        pvm.register_ctx_type(&CTX);
    }
}
```

### 4.2 (Optional) Implement `update`

The Mapped trait has a method `update` that allows for post deserialisation fix-up's to the trace records. For example in the CADETS mapping, some of the UUID values for system local objects were not globally unique, so as part of the `update` method they were mapped into globally unique identifiers by combining them with the UUID assigned to the host. If the method is not required it can be ommitted.

### 4.3 (Optional) Implement `set_offset`

The `set_offset` method allows the trace injestion system to inform a record of where it is located in the source trace. This information is often stashed and later used to construct part of the context information associated with the record.

#### Example
```rust
impl Mapped for Event {
    fn set_offset(&mut self, offset: u64) {
        self.trace_offset = Some(offset)
    }
}
```

### 4.4 Implement `process`

Finally the `process` method must be implemented. This method applies the mapping for a given record. This will use a series of the components we have previously defined. The general process can be broken down into 4 steps. First the context elements must be gathered, then a PVM transaction started, the sub mapping functions should be applied based on the type of record present, and finally the transaction should be committed or rolledback as appropriate.

#### Example
```rust
impl Mapped for Event {
    fn process(&self, pvm: &mut PVM) -> PVMResult<()> {
        // 1. Gather the context elements.
        let ctx_cont = HashMap::new();
        ctx_cont.insert("event_id", self.id.to_hyphenated_ref().to_string());
        if let Some(offset) = self.trace_offset {
            ctx_cont.insert("trace_offset", self.trace_offset.to_string());
        }

        // 2. Start a transaction
        let tr = pvm.transaction(&CTX, ctx_cont);

        // 3. Apply sub mapping functions

        let result = match &self.action[..] {
            "action::read" => self.map_read(&mut tr),
            "action::write" => self.map_write(&mut tr),
        }

        // 4. Commit or rollback as appropriate

        match result {
            Ok(_) => {
                tr.commit();
                Ok(())
            }
            Err(e) => {
                tr.rollback();
                Err(e)
            }
        }
    }
}
```
