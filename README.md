# StartupSpot Global — Find Your Perfect Business Location

## Setup Instructions

Follow these steps to set up the project on your local machine:

### 1. Prerequisites
- **Conda**: Ensure you have [Anaconda](https://www.anaconda.com/) installed.
- **Hugging Face Account**: You will need a Hugging Face account and a **Read Access Token** (https://huggingface.co/settings/tokens).

### 2. Environment Setup
Clone the repository and create the environment using the provided `environment.yml` file:

```bash
# Create the environment
conda env create -f environment.yml

# Activate the environment
conda activate hackathon
```

### 3. Data Retrieval (Required)
The complete dataset (`all_foursquare_locations.parquet`) is too large for GitHub and must be retrieved manually using the provided Jupyter notebook.

1. **Set up Credentials**: Create a file named `.env` in the root directory of the project and add your Hugging Face token:
   ```text
   HF_TOKEN=your_hugging_face_token_here
   ```
2. **Open the Notebook**: Launch Jupyter Notebook:
   ```bash
   jupyter notebook
   ```
3. **Run Data Retrieval Cells**: Open `data_cleaning.ipynb` and run the **first four code cells** in sequence, the last 2 are expected to take some time

*Note: You can ignore the subsequent cells related to "AU locations" as they are not required for the global website functionality and were simply considered during initial planning.*

---

## Running the Website

Once the `all_foursquare_locations.parquet` file is generated in the root directory, you can start the local web server:

1. Navigate to the `Web` directory:
   ```bash
   cd Web
   ```
2. Run the Flask application:
   ```bash
   python app.py
   ```
3. Open your browser and go to: [http://localhost:8080](http://localhost:8080)

---

## Accessing Jupyter Notebooks

To explore the data analysis or further clean the datasets, stay in the root directory and run:

```bash
jupyter notebook
```

You can then open `data_cleaning.ipynb`.
