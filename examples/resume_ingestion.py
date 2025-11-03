"""
Example of using HuggingFaceLoader with resume functionality.

This demonstrates how to properly synchronize state dict saves with data persistence.
"""

from dataset_db.ingestion.hf_loader import HuggingFaceLoader

# Initialize loader
loader = HuggingFaceLoader()

dataset_name = "example_dataset"

try:
    # Load dataset (automatically resumes if state dict exists)
    for batch in loader.load(dataset_name, resume=True):
        # Process the batch
        print(f"Processing batch with {len(batch)} rows")

        # Write data to disk (parquet, database, etc.)
        # ... your persistence logic here ...

        # IMPORTANT: Only save state dict AFTER data is successfully written
        loader.save_state_dict(dataset_name)

    # After successful completion, clean up state dict
    loader.clear_state_dict(dataset_name)
    print("Ingestion completed successfully")

except Exception as e:
    print(f"Ingestion failed: {e}")
    print(f"State dict saved at: {loader.get_state_dict_path(dataset_name)}")
    print("Resume by running the script again")
