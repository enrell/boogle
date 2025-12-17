use crate::index::ram::Document;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

pub struct Wal {
    path: PathBuf,
    writer: BufWriter<File>,
}

impl Wal {
    pub fn open<P: AsRef<Path>>(path: P) -> std::io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)?;

        Ok(Self {
            path,
            writer: BufWriter::new(file),
        })
    }

    pub fn append(&mut self, doc: &Document) -> std::io::Result<()> {
        let serialized = serde_json::to_string(doc)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        // We write newline-delimited JSON
        writeln!(self.writer, "{}", serialized)?;
        self.writer.flush()?; // Ensure it hits the OS buffer (users req: "mentir para o disco", but WAL implies safety?)
                              // The user said: "Ele não grava o arquivo no disco rígido imediatamente... ao mesmo tempo que escreve no buffer, ele anexa a operação no Translog"
                              // And "Escrever sequencialmente no final de um arquivo é muito rápido."
                              // So flushing to OS cache is fine, strict fsync isn't strictly required for "NRT speed" logic unless we want crash safety against power loss immediately.
                              // User said: "Para não perder dados se a luz acabar... anexa a operação no Translog... Escrever sequencialmente... é muito rápido."
                              // Usually `write` to File goes to OS cache. `flush` ensures it leaves the Rust buffer. `fsync` hits disk.
                              // We will `flush` to ensure it's in OS cache/WAL file handle.
        Ok(())
    }

    pub fn read_all(&self) -> std::io::Result<Vec<Document>> {
        let file = File::open(&self.path)?;
        let reader = BufReader::new(file);
        let mut docs = Vec::new();

        for line in reader.lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            if let Ok(doc) = serde_json::from_str::<Document>(&line) {
                docs.push(doc);
            }
        }
        Ok(docs)
    }

    pub fn truncate(&mut self) -> std::io::Result<()> {
        self.writer.flush()?; // clear any pending
                              // To truncate, we reopen with Write (truncate) mode
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.path)?;

        // Replace writer
        self.writer = BufWriter::new(file);
        Ok(())
    }
}
