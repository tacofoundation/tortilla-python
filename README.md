<img src="https://github.com/user-attachments/assets/16d84f7d-dee7-40d9-9990-d0f31bd1b028" width="20%" style="float: right; margin-left: 10px;">

[![Open in Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/drive/10Iqn9hPXXZBbnih59lV80C_9RT1ztEiG?usp=sharing)


# The Tortilla Reader

## What is Tortilla?

A Tortilla is an open-source, cloud-optimized format for storing large collections of files (referred to as "items") in a Binary Large Object (BLOB). It is quite simple, similar to [safetensor](https://huggingface.co/docs/safetensors/index) and [puffin](https://iceberg.apache.org/puffin-spec/), but designed for seamless compatibility with the Geospatial Data Abstraction Library (GDAL) [Virtual File System](https://gdal.org/en/latest/user/virtual_file_systems.html).

## Specification

Read the full specification [here](https://hackmd.io/@csaybar/B1QK_JERR).

<center>
    <img src="https://hackmd.io/_uploads/SkuqknA1Jg.png" width="85%">
</center>

## Installation

You can install the Tortilla reader using `pip`:

```bash
pip install pytortilla
```


## Usage

The `tortilla` reader only support three methods: `create`, `load`, and `compile`.

#### Create

Create a Tortilla file from a list of local files.

```python
import pytortilla

files = ["path/to/local/A.tif", "path/to/local/B.tif", "path/to/local/C.tif"]

tortilla_file = pytortilla.create(
    files=files,
    output= "path/to/local/demo.tortilla"
)
```

#### Load

Load local and remote Tortilla files. For remote files, the server must support the HTTP `Range` header method.


```python
import pytortilla

# either local or remote you can load a Tortilla file with the same function

## local
tortilla_file = pytortilla.load(
    file="path/to/local/demo.tortilla"
)

## remote
tortilla_file = pytortilla.load(
    file="https://huggingface.co/datasets/tacofoundation/example/resolve/main/soy_una.tortilla"
)


```

#### Compile

Create small subsets of the Tortilla file. For instance, if you have a Tortilla file with 1000 items, you can create a new Tortilla file with only the first 100 items.

```python
import pytortilla

# Load a Tortilla file
metadata = pytortilla.load(file="https://huggingface.co/datasets/tacofoundation/example/resolve/main/soy_una.tortilla")


# Create a new Tortilla file
subset = pytortilla.compile(
    dataset=metadata.iloc[40:50],
    output="soy_una_feliz.tortilla"
)
```