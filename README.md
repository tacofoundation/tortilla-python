
# 
<p align="center">
  <img src="https://raw.githubusercontent.com/tacofoundation/tacofoundation.github.io/refs/heads/main/public/tortilla.png" width="40%">
</p>


<p align="center">
    <em>The file format behind TACO.</em> 🫓
</p>

<p align="center">
  <!-- PyPI badge -->
  <a href='https://pypi.org/project/pytortilla'>
    <img src='https://img.shields.io/pypi/v/pytortilla.svg' alt='PyPI' />
  </a>
  <!-- License badge -->
  <a href="https://opensource.org/licenses/MIT" target="_blank">
    <img src="https://img.shields.io/badge/License-MIT-blue.svg" alt="License">
  </a>
  <!-- Black code style badge -->
  <a href="https://github.com/psf/black" target="_blank">
    <img src="https://img.shields.io/badge/code%20style-black-000000.svg" alt="Black">
  </a>
  <!-- isort badge -->
  <a href="https://pycqa.github.io/isort/" target="_blank">
    <img src="https://img.shields.io/badge/%20imports-isort-%231674b1?style=flat&labelColor=ef8336" alt="isort">
  </a>
</p>

---

**GitHub**: [https://github.com/tacofoundation/tortilla-python](https://github.com/tacofoundation/tortilla-python) 🌐

**PyPI**: [https://pypi.org/project/pytortilla/](https://pypi.org/project/pytortilla/) 🛠️




---

## **Tortilla 🫓**

Hello! I'm a Tortilla, a format to serialize your EO data 🤗.


**pytortilla** is a Python package that simplifies the creation and management of `.tortilla` files—these files are designed to encapsulate metadata, dataset information, and links to relevant files in remote sensing or AI workflows.

This package is “re-exported” within [`tacotoolbox`](https://github.com/tacotoolbox/tacotoolbox), specifically under `tacotoolbox.tortilla`. Therefore, by installing and using `pytortilla`, you can also leverage it from `tacotoolbox.tortilla`.


## **Goals**

1. **Metadata handling**: Defines classes (`Sample`, `Samples`) to describe and structure your data’s information.
2. **Dataset structuring**: Easily generate training, validation, and testing splits, and store them in `.tortilla` files.
3. **Internal validation**: Validate your dataset’s integrity (e.g., opening each file with [rasterio](https://rasterio.readthedocs.io)).
4. **Integration with Earth Engine (ee)**: Combines local data operations with GEE functionalities.
5. **Unified usage with** [`tacotoolbox`](https://github.com/tacotoolbox/tacotoolbox): Load and manipulate these datasets with `tacoreader` and other helper functions from `tacotoolbox`.



## **Table of Contents**

- [Installation](#installation)
- [Usage guide](#usage-guide)
- [Creating samples](#creating-samples)
- [Validation and adding metadata](#validation-and-adding-metadata)
- [Generating the .tortilla file](#generating-the-tortilla-file)
- [Loading and using the .tortilla File](#loading-and-using-the-tortilla-file)


## **Installation**

```bash
pip install pytortilla
```
or from source:

```bash
git clone https://github.com/tacofoundation/tortilla-python.git
cd tortilla-python
pip install .
```

*Note:* You may also install it as part of [`tacotoolbox`](https://github.com/tacofoundation/tacotoolbox), where `pytortilla` is included as a dependency.


## **Usage guide**

In this guide, we delve deeper into the step-by-step creation of `.tortilla` files, providing tips and best practices.


```python
import pathlib
import rasterio
import pandas as pd
from sklearn.model_selection import train_test_split
import pytortilla
```

If you need Earth Engine:

```python
import ee
ee.Initialize()  # Requires prior authentication if not done already
```

### **Files**

Move the Files from Hugging Face to Your Local Machine

```python
import os

# URL path to the Hugging Face repository
path = "https://huggingface.co/datasets/tacofoundation/tortilla_demo/resolve/main/"

# List of demo files to download
files = [
    "demo/high__test__ROI_0010__20190125T112341_20190125T112624_T28QFG.tif",
    "demo/high__test__ROI_0011__20190130T103251_20190130T104108_T31REP.tif",
    "demo/high__test__ROI_0011__20190830T102029_20190830T102552_T31REP.tif",
    "demo/high__test__ROI_0064__20190317T015619_20190317T020354_T51JVH.tif",
    "demo/high__test__ROI_0120__20191219T045209_20191219T045214_T45TXE.tif",
    "demo/high__test__ROI_0141__20190316T141049_20190316T142437_T19FDE.tif",
    "demo/high__test__ROI_0159__20200403T143721_20200403T144642_T19HBV.tif",
    "demo/high__test__ROI_0235__20200402T053639_20200402T053638_T44UNV.tif"
]

# Create a local folder called 'demo' (if not already existing)
os.system("mkdir -p demo")

# Download each file to the 'demo' folder
for file in files:
    os.system(f"wget {path}{file} -O {file}")
```

*Note:* Depending on your environment, you might prefer using requests or urllib instead of os.system for downloading files.

At this point, you should have a `demo/` folder populated with several `.tif` files.


### **Creating samples**

Now, we will create samples using `pytortilla`:

```python
import pathlib
import pandas as pd
from sklearn.model_selection import train_test_split
import rasterio
from pytortilla.datamodel import Sample, Samples

# Define the local path containing the TIFF files
demo_path = pathlib.Path("./demo")

# Collect all .tif files in the demo folder
all_files = list(demo_path.glob("*.tif"))

# Split into train, val, and test
train_files, test_files = train_test_split(all_files, test_size=0.2, random_state=42)
train_files, val_files = train_test_split(train_files, test_size=0.2, random_state=42)

train_df = pd.DataFrame({"path": train_files, "split": "train"})
val_df = pd.DataFrame({"path": val_files, "split": "validation"})
test_df = pd.DataFrame({"path": test_files, "split": "test"})
dataset_full = pd.concat([train_df, val_df, test_df], ignore_index=True)

# Build a list of Sample objects
samples_list = []
for _, row in dataset_full.iterrows():
    with rasterio.open(row.path) as src:
        metadata = src.profile
        sample_obj = Sample(
            id=row.path.stem,
            path=str(row.path),
            file_format="GTiff",
            data_split=row.split,
            stac_data={
                "crs": str(metadata["crs"]),
                "geotransform": metadata["transform"].to_gdal(),
                "raster_shape": (metadata["height"], metadata["width"])
            }
        )
        samples_list.append(sample_obj)

samples_obj = Samples(samples=samples_list)
```

### **Validation and adding metadata**

Validate each `.tif` file by trying to open it:

```python
samples_obj.deep_validator(read_function=lambda x: rasterio.open(x))
```

If you need RAI metadata (or any other additional metadata) in your workflow, you can include it:

```python
samples_obj = samples_obj.include_rai_metadata(
    sample_footprint=5120,  # Example footprint value
    cache=False,
    quiet=False
)
```
### **Generating the `.tortilla` file**

Use `pytortilla.create.main.create()` (or the equivalent `tacotoolbox.tortilla.create` if you have `tacotoolbox` installed):

```python
from pytortilla.create.main import create

# Generate the .tortilla file
output_file = create(
    samples=samples_obj,
    output="demo_dataset.tortilla"
)

print(f"Tortilla file generated: {output_file}")
```

The .tortilla might split into multiple files (`.0000.part.tortilla`, etc.) for large datasets.

### **Loading and using the `.tortilla` file**

Finally, load the `.tortilla` file (or its parts) with tacoreader:

```python
import tacoreader
import pandas as pd

dataset_chunks = []

# Try loading .part.tortilla files (assuming a maximum of 4 parts for this example)
for i in range(4):
    part_file = f"demo_dataset.{i:04d}.part.tortilla"
    try:
        dataset_part = tacoreader.load(part_file)
        dataset_chunks.append(dataset_part)
    except FileNotFoundError:
        break  # Stop if no more parts

if dataset_chunks:
    dataset = pd.concat(dataset_chunks, ignore_index=True)
    print(dataset.head())
else:
    print("No tortilla parts found.")
```
