#![allow(non_local_definitions)]

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use rustc_hash::FxHashMap;

mod profiler;
use profiler::{DalgaEngine, TypedValue};

#[pyclass]
struct Profiler {
    engine: DalgaEngine,
}
 
#[pymethods]
impl Profiler {
    #[new]
    fn new() -> Self {
        Profiler {
            engine: DalgaEngine::new(),
        }
    }

    fn observe_dict(&self, py_dict: &PyDict) -> PyResult<()> {
        let row = self.extract_row(py_dict)?;
        self.engine.process_batch(&[row]);
        Ok(())
    }

    fn observe_dicts(&self, py_list: &PyList) -> PyResult<()> {
        let mut batch = Vec::with_capacity(py_list.len());

        for item in py_list {
            if let Ok(py_dict) = item.downcast::<PyDict>() {
                batch.push(self.extract_row(py_dict)?);
            }
        }

        self.engine.process_batch(&batch);
        Ok(())
    }

    fn flush(&self) -> PyResult<String> {
        Ok(self.engine.flush_and_reset())
    }
}

impl Profiler {
    fn extract_row<'a>(&self, py_dict: &'a PyDict) -> PyResult<FxHashMap<String, TypedValue<'a>>> {
        let mut row_data = FxHashMap::default();

        for (k, v) in py_dict.iter() {
            let key_str = k.extract::<String>()?;

            let val = if v.is_none() {
                TypedValue::Null
            } else if let Ok(i) = v.extract::<i64>() {
                TypedValue::Int(i)
            } else if let Ok(f) = v.extract::<f64>() {
                if f.is_nan() {
                    TypedValue::Null
                } else {
                    TypedValue::Float(f)
                }
            } else if let Ok(s) = v.extract::<&str>() {
                TypedValue::Str(s)
            } else {
                TypedValue::Null
            };

            row_data.insert(key_str, val);
        }

        Ok(row_data)
    }
}

#[pymodule]
fn dalga(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<Profiler>()?;
    Ok(())
}
