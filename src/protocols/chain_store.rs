use crate::protocols::HeaderProvider;
use crate::store::{Batch, Error, IteratorDirection, Store};
use ckb_types::{
    core::{BlockNumber, HeaderView},
    packed,
    prelude::*,
    utilities::compact_to_difficulty,
    U256,
};
use std::convert::TryInto;
use std::sync::Arc;

pub enum Key {
    ActiveChain(BlockNumber),
    Header(packed::Byte32),
    OutPoint(packed::OutPoint),
    ConsumedOutPoint(packed::OutPoint),
    FilteredBlock(BlockNumber, packed::Byte32),
    Script(packed::Script),
}

#[repr(u8)]
pub enum KeyPrefix {
    ActiveChain = 224,
    Header = 192,
    OutPoint = 160,
    ConsumedOutPoint = 128,
    FilteredBlock = 96,
    Script = 64,
}

pub type IOIndex = u32;
pub enum IOType {
    Input,
    Output,
}
pub enum Value {
    ActiveChain(packed::Byte32),
    Header(packed::Header, U256),
    OutPoint(packed::CellOutput, packed::Bytes, BlockNumber),
    ConsumedOutPoint(
        packed::CellOutput,
        packed::Bytes,
        BlockNumber,
        packed::Byte32, // Consumed by which tx hash
        BlockNumber,    // Consumed by which block number
    ),
    FilteredBlock(Vec<(packed::Byte32, IOIndex, IOType)>),
    Script(BlockNumber),
}

impl Key {
    pub fn into_vec(self) -> Vec<u8> {
        self.into()
    }
}

impl Into<Vec<u8>> for Key {
    fn into(self) -> Vec<u8> {
        let mut encoded = Vec::new();

        match self {
            Key::ActiveChain(block_number) => {
                encoded.push(KeyPrefix::ActiveChain as u8);
                encoded.extend_from_slice(&block_number.to_be_bytes());
            }
            Key::Header(block_hash) => {
                encoded.push(KeyPrefix::Header as u8);
                encoded.extend_from_slice(block_hash.as_slice());
            }
            Key::OutPoint(out_point) => {
                encoded.push(KeyPrefix::OutPoint as u8);
                encoded.extend_from_slice(out_point.as_slice());
            }
            Key::ConsumedOutPoint(out_point) => {
                encoded.push(KeyPrefix::ConsumedOutPoint as u8);
                encoded.extend_from_slice(out_point.as_slice());
            }
            Key::FilteredBlock(block_number, block_hash) => {
                encoded.push(KeyPrefix::FilteredBlock as u8);
                encoded.extend_from_slice(&block_number.to_be_bytes());
                encoded.extend_from_slice(block_hash.as_slice());
            }
            Key::Script(script) => {
                encoded.push(KeyPrefix::Script as u8);
                encoded.extend_from_slice(script.as_slice());
            }
        }
        encoded
    }
}

impl Into<Vec<u8>> for Value {
    fn into(self) -> Vec<u8> {
        let mut encoded = Vec::new();
        match self {
            Value::ActiveChain(block_hash) => {
                encoded.extend_from_slice(block_hash.as_slice());
            }
            Value::Header(header, total_difficulty) => {
                encoded.extend_from_slice(header.as_slice());
                encoded.extend_from_slice(total_difficulty.pack().as_slice());
            }
            Value::OutPoint(output, output_data, block_number) => {
                encoded.extend_from_slice(output.as_slice());
                encoded.extend_from_slice(output_data.as_slice());
                encoded.extend_from_slice(&block_number.to_be_bytes());
            }
            Value::ConsumedOutPoint(
                output,
                output_data,
                block_number,
                consumed_by_tx_hash,
                consumed_by_block_number,
            ) => {
                encoded.extend_from_slice(output.as_slice());
                encoded.extend_from_slice(output_data.as_slice());
                encoded.extend_from_slice(&block_number.to_be_bytes());
                encoded.extend_from_slice(consumed_by_tx_hash.as_slice());
                encoded.extend_from_slice(&consumed_by_block_number.to_be_bytes());
            }
            Value::FilteredBlock(ios) => {
                for (tx_hash, io_index, io_type) in ios {
                    encoded.extend_from_slice(tx_hash.as_slice());
                    encoded.extend_from_slice(&io_index.to_be_bytes());
                    match io_type {
                        IOType::Input => encoded.push(0),
                        IOType::Output => encoded.push(1),
                    }
                }
            }
            Value::Script(block_number) => {
                encoded.extend_from_slice(&block_number.to_be_bytes());
            }
        }
        encoded
    }
}

#[derive(Clone)]
pub struct ChainStore<S> {
    pub store: Arc<S>,
}

impl<S: Store> ChainStore<S> {
    pub fn tip(&self) -> Result<Option<HeaderView>, Error> {
        let mut iter = self
            .store
            .iter(
                &[KeyPrefix::ActiveChain as u8 + 1],
                IteratorDirection::Reverse,
            )?
            .take_while(|(key, _value)| key.starts_with(&[KeyPrefix::ActiveChain as u8]));

        if let Some(tip_hash) = iter.next().map(|(_key, value)| {
            packed::Byte32Reader::from_slice_should_be_ok(&value[..]).to_entity()
        }) {
            self.get_header(tip_hash)
        } else {
            Ok(None)
        }
    }

    pub fn get_header(&self, block_hash: packed::Byte32) -> Result<Option<HeaderView>, Error> {
        self.store
            .get(&Key::Header(block_hash.clone()).into_vec())
            .map(|value| {
                value.map(|raw| {
                    packed::HeaderView::new_builder()
                        .data(
                            packed::HeaderReader::from_slice_should_be_ok(
                                &raw[..packed::Header::TOTAL_SIZE],
                            )
                            .to_entity(),
                        )
                        .hash(block_hash)
                        .build()
                        .unpack()
                })
            })
    }

    fn get_total_difficulty(&self, block_hash: packed::Byte32) -> Result<Option<U256>, Error> {
        self.store
            .get(&Key::Header(block_hash).into_vec())
            .map(|value| {
                value.map(|raw| {
                    U256::from_little_endian(&raw[packed::Header::TOTAL_SIZE..])
                        .expect("stored total difficulty")
                })
            })
    }

    pub fn get_block_hash(
        &self,
        block_number: BlockNumber,
    ) -> Result<Option<packed::Byte32>, Error> {
        self.store
            .get(&Key::ActiveChain(block_number).into_vec())
            .map(|value| {
                value.map(|raw| packed::Byte32Reader::from_slice_should_be_ok(&raw[..]).to_entity())
            })
    }

    pub fn init(&self, genesis: HeaderView) -> Result<(), Error> {
        let mut batch = self.store.batch()?;
        batch.put_kv(
            Key::Header(genesis.hash()),
            Value::Header(
                genesis.data(),
                compact_to_difficulty(genesis.compact_target()),
            ),
        )?;
        batch.put_kv(Key::ActiveChain(0), Value::ActiveChain(genesis.hash()))?;
        batch.commit()
    }

    pub fn insert_header(&self, header: HeaderView) -> Result<(), Error> {
        let mut batch = self.store.batch()?;
        let parent_total_difficulty = self
            .get_total_difficulty(header.parent_hash())?
            .expect("verified parent hash");
        let total_difficulty =
            parent_total_difficulty + compact_to_difficulty(header.compact_target());
        batch.put_kv(
            Key::Header(header.hash()),
            Value::Header(header.data(), total_difficulty.clone()),
        )?;
        let tip = self.tip()?.expect("stored tip");
        if header.parent_hash() == tip.hash() {
            batch.put_kv(
                Key::ActiveChain(header.number()),
                Value::ActiveChain(header.hash()),
            )?;
        } else {
            let tip_total_difficulty = self.get_total_difficulty(tip.hash())?.expect("stored tip");
            if total_difficulty > tip_total_difficulty {
                for number in header.number()..=tip.number() {
                    batch.delete(Key::ActiveChain(number).into_vec())?;
                }

                let mut current_header = header;
                loop {
                    batch.put_kv(
                        Key::ActiveChain(current_header.number()),
                        Value::ActiveChain(current_header.hash()),
                    )?;
                    if self
                        .get_block_hash(current_header.number() - 1)?
                        .expect("stored active chain")
                        == current_header.parent_hash()
                    {
                        break;
                    } else {
                        current_header = self
                            .get_header(current_header.parent_hash())?
                            .expect("stored parent header");
                    }
                }
            }
        }

        batch.commit()
    }

    pub fn get_locator(&self) -> Result<Vec<packed::Byte32>, Error> {
        let mut locator = Vec::with_capacity(32);
        let mut block_number = self.tip()?.expect("stored tip").number();
        let mut step = 1;

        loop {
            locator.push(
                self.get_block_hash(block_number)?
                    .expect("stored block hash"),
            );

            if locator.len() >= 10 {
                step <<= 1;
            }

            if block_number > step {
                block_number -= step;
            } else {
                if block_number > 0 {
                    locator.push(self.get_block_hash(0)?.expect("stored block hash"));
                }
                break;
            }
        }
        Ok(locator)
    }

    pub fn append_filtered_block(
        &self,
        filtered_block: packed::FilteredBlock,
    ) -> Result<(), Error> {
        let scripts = self
            .get_scripts()?
            .into_iter()
            .map(|(script, _block_number)| script)
            .collect::<Vec<_>>();
        let mut batch = self.store.batch()?;
        let mut matched = Vec::new();
        if let Some(filtered_txs) = filtered_block.transactions().to_opt() {
            for tx in filtered_txs.transactions() {
                for (index, input) in tx.raw().inputs().into_iter().enumerate() {
                    if let Some((output, output_data, block_number)) =
                        self.get_out_point(input.previous_output())?
                    {
                        if scripts.iter().any(|script| script == &output.lock()) {
                            let tx_hash = input.previous_output().tx_hash();
                            matched.push((tx_hash.clone(), index as u32, IOType::Input));
                            batch.put_kv(
                                Key::ConsumedOutPoint(packed::OutPoint::new(tx_hash, index as u32)),
                                Value::ConsumedOutPoint(
                                    output,
                                    output_data,
                                    block_number,
                                    tx.calc_tx_hash(),
                                    filtered_block.header().raw().number().unpack(),
                                ),
                            )?;
                            batch.delete(Key::OutPoint(input.previous_output()).into_vec())?;
                        }
                    }
                }
                for (index, output) in tx.raw().outputs().into_iter().enumerate() {
                    if scripts.iter().any(|script| script == &output.lock()) {
                        let tx_hash = tx.calc_tx_hash();
                        matched.push((tx_hash.clone(), index as u32, IOType::Output));
                        batch.put_kv(
                            Key::OutPoint(packed::OutPoint::new(tx_hash, index as u32)),
                            Value::OutPoint(
                                output,
                                tx.raw().outputs_data().get(index).expect("checked len"),
                                filtered_block.header().raw().number().unpack(),
                            ),
                        )?;
                    }
                }
            }
        };
        let header = filtered_block.header().into_view();
        batch.put_kv(
            Key::FilteredBlock(header.number(), header.hash()),
            Value::FilteredBlock(matched),
        )?;
        batch.commit()?;
        self.insert_header(header)
    }

    pub fn insert_filtered_blocks(
        &self,
        filtered_blocks: packed::FilteredBlocks,
    ) -> Result<(), Error> {
        let scripts = self
            .get_scripts()?
            .into_iter()
            .map(|(script, _block_number)| script)
            .collect::<Vec<_>>();
        let mut batch = self.store.batch()?;
        let mut last_number = 0;
        for block_hash in filtered_blocks.unmatched_block_hashes() {
            if let Some(header) = self.get_header(block_hash.clone())? {
                batch.put_kv(
                    Key::FilteredBlock(header.number(), block_hash),
                    Value::FilteredBlock(Vec::new()),
                )?;
                last_number = header.number();
            }
        }
        for (index, matched_block) in filtered_blocks.matched_blocks().into_iter().enumerate() {
            let block_hash = filtered_blocks
                .matched_block_hashes()
                .get(index)
                .expect("checked len");

            if let Some(header) = self.get_header(block_hash.clone())? {
                let mut matched = Vec::new();
                for tx in matched_block.transactions() {
                    for (index, input) in tx.raw().inputs().into_iter().enumerate() {
                        if let Some((output, output_data, block_number)) =
                            self.get_out_point(input.previous_output())?
                        {
                            if scripts.iter().any(|script| script == &output.lock()) {
                                let tx_hash = input.previous_output().tx_hash();
                                matched.push((tx_hash.clone(), index as u32, IOType::Input));
                                batch.put_kv(
                                    Key::ConsumedOutPoint(packed::OutPoint::new(
                                        tx_hash,
                                        index as u32,
                                    )),
                                    Value::ConsumedOutPoint(
                                        output,
                                        output_data,
                                        block_number,
                                        tx.calc_tx_hash(),
                                        header.number(),
                                    ),
                                )?;
                                batch.delete(Key::OutPoint(input.previous_output()).into_vec())?;
                            }
                        }
                    }
                    for (index, output) in tx.raw().outputs().into_iter().enumerate() {
                        if scripts.iter().any(|script| script == &output.lock()) {
                            let tx_hash = tx.calc_tx_hash();
                            matched.push((tx_hash.clone(), index as u32, IOType::Output));
                            batch.put_kv(
                                Key::OutPoint(packed::OutPoint::new(tx_hash, index as u32)),
                                Value::OutPoint(
                                    output,
                                    tx.raw().outputs_data().get(index).expect("checked len"),
                                    header.number(),
                                ),
                            )?;
                        }
                    }
                }
                batch.put_kv(
                    Key::FilteredBlock(header.number(), block_hash),
                    Value::FilteredBlock(matched),
                )?;
                last_number = header.number();
            }
        }

        if last_number > 0 {
            for script in scripts {
                batch.put_kv(Key::Script(script), Value::Script(last_number))?;
            }
        }

        batch.commit()
    }

    fn get_out_point(
        &self,
        out_point: packed::OutPoint,
    ) -> Result<Option<(packed::CellOutput, packed::Bytes, BlockNumber)>, Error> {
        self.store
            .get(&Key::OutPoint(out_point).into_vec())
            .map(|value| {
                value.map(|raw| {
                    let output_size = u32::from_le_bytes(
                        raw[..4]
                            .try_into()
                            .expect("stored OutPoint value: output_size"),
                    ) as usize;
                    let output = packed::CellOutput::from_slice(&raw[..output_size])
                        .expect("stored OutPoint value: output");
                    let output_data = packed::Bytes::from_slice(&raw[output_size..raw.len() - 8])
                        .expect("stored OutPoint value: output_data");
                    let block_number = BlockNumber::from_be_bytes(
                        raw[raw.len() - 8..]
                            .try_into()
                            .expect("stored OutPoint value: block_number"),
                    );
                    (output, output_data, block_number)
                })
            })
    }

    fn get_consumed_out_point(
        &self,
        out_point: packed::OutPoint,
    ) -> Result<Option<(packed::CellOutput, packed::Bytes, BlockNumber)>, Error> {
        self.store
            .get(&Key::ConsumedOutPoint(out_point).into_vec())
            .map(|value| {
                value.map(|raw| {
                    let output_size = u32::from_le_bytes(
                        raw[..4]
                            .try_into()
                            .expect("stored ConsumedOutPoint output_size"),
                    ) as usize;
                    let output = packed::CellOutput::from_slice(&raw[..output_size])
                        .expect("stored ConsumedOutPoint output");
                    let output_data = packed::Bytes::from_slice(&raw[output_size..raw.len() - 48])
                        .expect("stored ConsumedOutPoint output_data");
                    let created_by_block_number = BlockNumber::from_be_bytes(
                        raw[raw.len() - 48..raw.len() - 40]
                            .try_into()
                            .expect("stored ConsumedOutPoint value: created_by_block_number"),
                    );
                    (output, output_data, created_by_block_number)
                })
            })
    }

    pub fn get_unfiltered_block_hashes(&self, limit: usize) -> Result<Vec<packed::Byte32>, Error> {
        let mut start_number = self
            .get_scripts()?
            .iter()
            .map(|(_script, block_number)| block_number)
            .min()
            .cloned()
            .unwrap_or(0);
        // check fork and find unfiltered block start number
        {
            let mut start_key = Vec::new();
            start_key.push(KeyPrefix::FilteredBlock as u8);
            start_key.extend_from_slice(&(start_number + 1).to_be_bytes());
            let iter = self
                .store
                .iter(&start_key, IteratorDirection::Reverse)?
                .take_while(|(key, _value)| key.starts_with(&[KeyPrefix::FilteredBlock as u8]));

            for (key, _value) in iter {
                let filtered_block_number =
                    BlockNumber::from_be_bytes(key[1..9].try_into().expect("stored block number"));
                let filtered_block_hash =
                    packed::Byte32::from_slice(&key[9..]).expect("stored block hash");
                if self.get_block_hash(filtered_block_number)? == Some(filtered_block_hash.clone())
                {
                    start_number = filtered_block_number + 1;
                    break;
                } else {
                    self.rollback_filtered_block(filtered_block_number, filtered_block_hash)?;
                }
            }
        }

        self.store
            .iter(
                &Key::ActiveChain(start_number).into_vec(),
                IteratorDirection::Forward,
            )
            .map(|iter| {
                iter.take_while(|(key, _value)| key.starts_with(&[KeyPrefix::ActiveChain as u8]))
                    .take(limit)
                    .map(|(_key, value)| {
                        packed::Byte32::from_slice(&value).expect("stored block hash")
                    })
                    .collect::<Vec<_>>()
            })
    }

    fn rollback_filtered_block(
        &self,
        block_number: BlockNumber,
        block_hash: packed::Byte32,
    ) -> Result<(), Error> {
        let mut batch = self.store.batch()?;
        if let Some(matched) = self
            .store
            .get(&Key::FilteredBlock(block_number, block_hash.clone()).into_vec())
            .map(|value| {
                value.map(|raw| {
                    raw.chunks_exact(37)
                        .map(|s| {
                            (
                                packed::Byte32::from_slice(&s[0..32])
                                    .expect("stored FilteredBlock value: tx_hash"),
                                IOIndex::from_be_bytes(
                                    s[32..36]
                                        .try_into()
                                        .expect("stored FilteredBlock value: index"),
                                ),
                                if s[36] == 0 {
                                    IOType::Input
                                } else {
                                    IOType::Output
                                },
                            )
                        })
                        .collect::<Vec<_>>()
                })
            })?
        {
            for (tx_hash, io_index, io_type) in matched.into_iter().rev() {
                let out_point = packed::OutPoint::new(tx_hash, io_index);
                match io_type {
                    IOType::Input => {
                        if let Some((output, output_data, created_by_block_number)) =
                            self.get_consumed_out_point(out_point.clone())?
                        {
                            batch.delete(Key::ConsumedOutPoint(out_point.clone()).into_vec())?;
                            batch.put_kv(
                                Key::OutPoint(out_point),
                                Value::OutPoint(output, output_data, created_by_block_number),
                            )?;
                        }
                    }
                    IOType::Output => {
                        batch.delete(Key::OutPoint(out_point).into_vec())?;
                    }
                }
            }
            batch.delete(Key::FilteredBlock(block_number, block_hash).into_vec())?;
        }

        batch.commit()
    }

    pub fn insert_script(
        &self,
        script: packed::Script,
        block_number: BlockNumber,
    ) -> Result<(), Error> {
        let mut batch = self.store.batch()?;
        batch.put_kv(Key::Script(script), Value::Script(block_number))?;
        batch.commit()
    }

    pub fn get_scripts(&self) -> Result<Vec<(packed::Script, BlockNumber)>, Error> {
        self.store
            .iter(&[KeyPrefix::Script as u8], IteratorDirection::Forward)
            .map(|iter| {
                iter.take_while(|(key, _value)| key.starts_with(&[KeyPrefix::Script as u8]))
                    .map(|(key, value)| {
                        (
                            packed::ScriptReader::from_slice_should_be_ok(&key[1..]).to_entity(),
                            BlockNumber::from_be_bytes(
                                value[0..8]
                                    .try_into()
                                    .expect("stored Script value: block_number"),
                            ),
                        )
                    })
                    .collect::<Vec<_>>()
            })
    }

    pub fn get_cells(
        &self,
        script: &packed::Script,
    ) -> Result<
        Vec<(
            packed::OutPoint,
            packed::CellOutput,
            packed::Bytes,
            BlockNumber,
        )>,
        Error,
    > {
        self.store
            .iter(&[KeyPrefix::OutPoint as u8], IteratorDirection::Forward)
            .map(|iter| {
                iter.take_while(|(key, _value)| key.starts_with(&[KeyPrefix::OutPoint as u8]))
                    .filter_map(|(key, value)| {
                        let output_size = u32::from_le_bytes(
                            value[..4]
                                .try_into()
                                .expect("stored OutPoint value: output_size"),
                        ) as usize;
                        let output = packed::CellOutput::from_slice(&value[..output_size])
                            .expect("stored OutPoint value: output");
                        if script.eq(&output.lock()) {
                            let out_point = packed::OutPoint::from_slice(&key[1..])
                                .expect("stored OutPoint key");
                            let output_data =
                                packed::Bytes::from_slice(&value[output_size..value.len() - 8])
                                    .expect("stored OutPoint value: output_data");
                            let block_number = BlockNumber::from_be_bytes(
                                value[value.len() - 8..]
                                    .try_into()
                                    .expect("stored OutPoint value: block_number"),
                            );
                            Some((out_point, output, output_data, block_number))
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>()
            })
    }

    pub fn get_consumed_cells(
        &self,
        script: &packed::Script,
    ) -> Result<
        Vec<(
            packed::OutPoint,
            packed::CellOutput,
            packed::Bytes,
            BlockNumber,
            packed::Byte32,
            BlockNumber,
        )>,
        Error,
    > {
        self.store
            .iter(
                &[KeyPrefix::ConsumedOutPoint as u8],
                IteratorDirection::Forward,
            )
            .map(|iter| {
                iter.take_while(|(key, _value)| {
                    key.starts_with(&[KeyPrefix::ConsumedOutPoint as u8])
                })
                .filter_map(|(key, value)| {
                    let output_size = u32::from_le_bytes(
                        value[..4]
                            .try_into()
                            .expect("stored ConsumedOutPoint value: output_size"),
                    ) as usize;
                    let output = packed::CellOutput::from_slice(&value[..output_size])
                        .expect("stored ConsumedOutPoint value: output");
                    if script.eq(&output.lock()) {
                        let out_point = packed::OutPoint::from_slice(&key[1..])
                            .expect("stored ConsumedOutPoint key");
                        let output_data =
                            packed::Bytes::from_slice(&value[output_size..value.len() - 48])
                                .expect("stored ConsumedOutPoint value: output_data");
                        let created_by_block_number = BlockNumber::from_be_bytes(
                            value[value.len() - 48..value.len() - 40]
                                .try_into()
                                .expect("stored ConsumedOutPoint value: created_by_block_number"),
                        );
                        let consumed_by_tx_hash =
                            packed::Byte32::from_slice(&value[value.len() - 40..value.len() - 8])
                                .expect("stored ConsumedOutPoint value: tx_hash");
                        let consumed_by_block_number = BlockNumber::from_be_bytes(
                            value[value.len() - 8..]
                                .try_into()
                                .expect("stored ConsumedOutPoint value: consumed_by_block_number"),
                        );
                        Some((out_point, output, output_data, created_by_block_number, consumed_by_tx_hash, consumed_by_block_number))
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>()
            })
    }
}

pub struct HeaderProviderWrapper<'a, S> {
    pub store: &'a ChainStore<S>,
}

impl<'a, S: Store> HeaderProvider for HeaderProviderWrapper<'a, S> {
    fn get_header(&self, hash: packed::Byte32) -> Option<HeaderView> {
        self.store.get_header(hash).expect("store should be OK")
    }
}
