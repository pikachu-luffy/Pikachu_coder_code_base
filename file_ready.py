# import polars as pl
# import pandas as pd
# from polars import DataFrame


# age_data_df = pl.read_csv('age_data.csv', infer_schema_length=10000000, ignore_errors=True)
# print(len(age_data_df))
# age_data_df_sample_df  = age_data_df.head(4000)


# age_data_df_sample_df.write_csv('age_data_sample.csv', include_header=True, separator=',')




# age_data_pandas_df = pd.read_csv('age_data.csv', nrows=10000000, low_memory=False)
# print(age_data_pandas_df.head(10))



# gr_source_data_df = pl.read_csv('source_data.csv', infer_schema_length=10000000, ignore_errors=True)
# print(len(gr_source_data_df))
# gr_source_data_df_sample_df  = gr_source_data_df.head(4000)


# gr_source_data_df_sample_df.write_csv('gr_source_data_sample.csv', include_header=True, separator=',')





import polars as pl
import pandas as pd
from polars import DataFrame
import os
from pathlib import Path
import math

def chunk_csv_file(input_file: str, chunk_size: int = 1000000, output_dir: str = None):
    """
    Split a large CSV file into smaller chunks using Polars.
    
    Args:
        input_file (str): Path to the input CSV file
        chunk_size (int): Number of rows per chunk (default: 1,000,000)
        output_dir (str): Directory to save chunk files (auto-generated if None)
    
    Returns:
        dict: Summary information about the chunking process
    """
    
    # Auto-generate output directory name if not provided
    if output_dir is None:
        base_name = os.path.splitext(os.path.basename(input_file))[0]
        output_dir = f"{base_name}_chunks"
    
    # Create output directory if it doesn't exist
    Path(output_dir).mkdir(exist_ok=True)
    
    print(f"\nStarting to process file: {input_file}")
    print(f"Output directory: {output_dir}")
    
    # First, get the total number of rows by reading the entire file efficiently
    print("Counting total rows...")
    try:
        # Use lazy loading to count rows efficiently
        total_rows = pl.scan_csv(
            input_file,
            infer_schema_length=100000000,
            ignore_errors=True
        ).select(pl.len()).collect().item()
        
        print(f"Total rows in file: {total_rows:,}")
    except Exception as e:
        print(f"Error counting rows with lazy loading: {e}")
        print("Falling back to batched counting...")
        
        # Fallback: Count rows using batched reading
        total_rows = 0
        batch_size = 100000
        reader = pl.read_csv_batched(
            input_file, 
            batch_size=batch_size,
            infer_schema_length=100000000,
            ignore_errors=True
        )
        
        # Read batches one by one to count rows
        while True:
            try:
                batches = reader.next_batches(1)
                if not batches or len(batches) == 0:
                    break
                total_rows += len(batches[0])
            except Exception as batch_error:
                print(f"Error reading batch: {batch_error}")
                break
        
        print(f"Total rows in file (counted via batches): {total_rows:,}")
    
    if total_rows == 0:
        print("❌ No rows found in the file or unable to read the file.")
        return None
    
    # Calculate number of chunks needed
    num_chunks = math.ceil(total_rows / chunk_size)
    print(f"Will create {num_chunks} chunks of up to {chunk_size:,} rows each")
    
    # Process chunks
    chunk_info = []
    processed_rows = 0
    
    # Create a new reader for chunking
    reader = pl.read_csv_batched(
        input_file,
        batch_size=chunk_size,
        infer_schema_length=100000000,
        ignore_errors=True
    )
    
    chunk_num = 1
    base_filename = os.path.splitext(os.path.basename(input_file))[0]
    
    while processed_rows < total_rows:
        try:
            # Read next batch (one at a time)
            batches = reader.next_batches(1)
            if not batches or len(batches) == 0:
                print("No more batches to read.")
                break
                
            chunk_df = batches[0]
            
            if len(chunk_df) == 0:
                print("Empty batch encountered.")
                break
            
            # Define row range for this chunk
            start_row = processed_rows + 1
            end_row = processed_rows + len(chunk_df)
            
            # Create output filename using the base filename
            output_file = os.path.join(output_dir, f"{base_filename}_chunk_{chunk_num:03d}.csv")
            
            # Save chunk
            chunk_df.write_csv(output_file)
            
            # Track chunk information
            chunk_info.append({
                'chunk_number': chunk_num,
                'filename': output_file,
                'start_row': start_row,
                'end_row': end_row,
                'row_count': len(chunk_df)
            })
            
            processed_rows += len(chunk_df)
            
            print(f"Chunk {chunk_num}: Rows {start_row:,} to {end_row:,} "
                  f"({len(chunk_df):,} rows) -> {output_file}")
            
            chunk_num += 1
            
        except Exception as e:
            print(f"Error processing chunk {chunk_num}: {e}")
            break
    
    # Validation: Check if all rows were processed
    if processed_rows == total_rows:
        print(f"\n✅ SUCCESS: All {total_rows:,} rows have been processed into {len(chunk_info)} chunks")
    else:
        print(f"\n❌ WARNING: Expected {total_rows:,} rows but processed {processed_rows:,} rows")
    
    # Create summary report
    summary = {
        'input_file': input_file,
        'total_rows': total_rows,
        'processed_rows': processed_rows,
        'chunk_size': chunk_size,
        'num_chunks': len(chunk_info),
        'output_directory': output_dir,
        'chunks': chunk_info
    }
    
    # Save summary to file
    summary_file = os.path.join(output_dir, f"{base_filename}_chunking_summary.txt")
    with open(summary_file, 'w') as f:
        f.write(f"CSV Chunking Summary for {input_file}\n")
        f.write(f"=" * 50 + "\n\n")
        f.write(f"Input file: {input_file}\n")
        f.write(f"Total rows: {total_rows:,}\n")
        f.write(f"Processed rows: {processed_rows:,}\n")
        f.write(f"Chunk size: {chunk_size:,}\n")
        f.write(f"Number of chunks: {len(chunk_info)}\n")
        f.write(f"Output directory: {output_dir}\n\n")
        f.write(f"Chunk Details:\n")
        f.write(f"-" * 80 + "\n")
        
        for chunk in chunk_info:
            f.write(f"Chunk {chunk['chunk_number']:3d}: "
                   f"Rows {chunk['start_row']:8,} to {chunk['end_row']:8,} "
                   f"({chunk['row_count']:8,} rows) -> {chunk['filename']}\n")
    
    print(f"Summary saved to: {summary_file}")
    
    return summary

def verify_chunks(output_dir: str, file_prefix: str):
    """
    Verify that chunks contain all data and no overlaps exist.
    
    Args:
        output_dir (str): Directory containing chunk files
        file_prefix (str): Prefix of chunk files to verify
    """
    print(f"\nVerifying chunks in directory: {output_dir}")
    
    chunk_files = sorted([f for f in os.listdir(output_dir) 
                         if f.startswith(f"{file_prefix}_chunk_") and f.endswith(".csv")])
    
    if not chunk_files:
        print(f"No chunk files found with prefix '{file_prefix}_chunk_'!")
        return
    
    total_rows = 0
    expected_start = 1
    
    for i, chunk_file in enumerate(chunk_files, 1):
        chunk_path = os.path.join(output_dir, chunk_file)
        try:
            chunk_df = pl.read_csv(chunk_path)
            chunk_rows = len(chunk_df)
            total_rows += chunk_rows
            
            expected_end = expected_start + chunk_rows - 1
            
            print(f"Chunk {i}: {chunk_file} - Rows {expected_start:,} to {expected_end:,} ({chunk_rows:,} rows)")
            
            expected_start = expected_end + 1
        except Exception as e:
            print(f"Error reading chunk {chunk_file}: {e}")
    
    print(f"Total rows across all {file_prefix} chunks: {total_rows:,}")

def process_multiple_files(files_config: list):
    """
    Process multiple CSV files for chunking.
    
    Args:
        files_config (list): List of dictionaries with file configuration
                           Each dict should have: 'file', 'chunk_size', 'output_dir' (optional)
    """
    results = {}
    
    for config in files_config:
        input_file = config['file']
        chunk_size = config.get('chunk_size', 1000000)
        output_dir = config.get('output_dir', None)
        
        if not os.path.exists(input_file):
            print(f"❌ Error: Input file '{input_file}' not found!")
            continue
        
        try:
            print(f"\n{'='*60}")
            print(f"Processing: {input_file}")
            print(f"{'='*60}")
            
            # Chunk the file
            summary = chunk_csv_file(
                input_file=input_file,
                chunk_size=chunk_size,
                output_dir=output_dir
            )
            
            if summary is None:
                print(f"❌ Failed to process {input_file}")
                continue
            
            # Verify the chunks
            base_filename = os.path.splitext(os.path.basename(input_file))[0]
            verify_chunks(summary['output_directory'], base_filename)
            
            results[input_file] = summary
            
            print(f"\n🎉 {input_file} chunking completed successfully!")
            print(f"📁 Output directory: {summary['output_directory']}")
            print(f"📊 Total chunks created: {summary['num_chunks']}")
            
        except Exception as e:
            print(f"❌ Error during chunking {input_file}: {e}")
            import traceback
            traceback.print_exc()
    
    return results

def create_sample_files():
    """Create sample files from the original large CSV files."""
    print("Creating sample files...")
    
    # Process age_data.csv
    try:
        print("Processing age_data.csv for sample...")
        age_data_df = pl.read_csv('age_data.csv', infer_schema_length=10000000, ignore_errors=True)
        print(f"Age data total rows: {len(age_data_df):,}")
        
        age_data_sample_df = age_data_df.head(4000)
        age_data_sample_df.write_csv('age_data_sample.csv', include_header=True, separator=',')
        print("✅ Created age_data_sample.csv")
        
    except Exception as e:
        print(f"❌ Error processing age_data.csv: {e}")
    
    # Process source_data.csv
    try:
        print("Processing source_data.csv for sample...")
        gr_source_data_df = pl.read_csv('source_data.csv', infer_schema_length=10000000, ignore_errors=True)
        print(f"Source data total rows: {len(gr_source_data_df):,}")
        
        gr_source_data_sample_df = gr_source_data_df.head(4000)
        gr_source_data_sample_df.write_csv('gr_source_data_sample.csv', include_header=True, separator=',')
        print("✅ Created gr_source_data_sample.csv")
        
    except Exception as e:
        print(f"❌ Error processing source_data.csv: {e}")

# Alternative approach using streaming for very large files
def chunk_csv_file_streaming(input_file: str, chunk_size: int = 1000000, output_dir: str = None):
    """
    Alternative streaming approach for very large files that might not fit in memory.
    """
    if output_dir is None:
        base_name = os.path.splitext(os.path.basename(input_file))[0]
        output_dir = f"{base_name}_chunks"
    
    Path(output_dir).mkdir(exist_ok=True)
    
    print(f"\nStarting streaming process for file: {input_file}")
    print(f"Output directory: {output_dir}")
    
    chunk_info = []
    chunk_num = 1
    processed_rows = 0
    base_filename = os.path.splitext(os.path.basename(input_file))[0]
    
    try:
        # Use streaming with scan_csv and collect in chunks
        lazy_df = pl.scan_csv(
            input_file,
            infer_schema_length=10000000,
            ignore_errors=True
        )
        
        # Process in chunks using offset and limit
        offset = 0
        
        while True:
            try:
                # Read chunk using lazy operations
                chunk_df = lazy_df.slice(offset, chunk_size).collect()
                
                if len(chunk_df) == 0:
                    break
                
                # Define row range for this chunk
                start_row = processed_rows + 1
                end_row = processed_rows + len(chunk_df)
                
                # Create output filename
                output_file = os.path.join(output_dir, f"{base_filename}_chunk_{chunk_num:03d}.csv")
                
                # Save chunk
                chunk_df.write_csv(output_file)
                
                # Track chunk information
                chunk_info.append({
                    'chunk_number': chunk_num,
                    'filename': output_file,
                    'start_row': start_row,
                    'end_row': end_row,
                    'row_count': len(chunk_df)
                })
                
                processed_rows += len(chunk_df)
                
                print(f"Chunk {chunk_num}: Rows {start_row:,} to {end_row:,} "
                      f"({len(chunk_df):,} rows) -> {output_file}")
                
                chunk_num += 1
                offset += chunk_size
                
                # If we got less than chunk_size rows, we're done
                if len(chunk_df) < chunk_size:
                    break
                    
            except Exception as e:
                print(f"Error processing chunk {chunk_num}: {e}")
                break
        
        print(f"\n✅ Streaming process completed: {processed_rows:,} rows processed into {len(chunk_info)} chunks")
        
        return {
            'input_file': input_file,
            'total_rows': processed_rows,
            'processed_rows': processed_rows,
            'chunk_size': chunk_size,
            'num_chunks': len(chunk_info),
            'output_directory': output_dir,
            'chunks': chunk_info
        }
        
    except Exception as e:
        print(f"Error in streaming approach: {e}")
        return None

# Main execution
if __name__ == "__main__":
    print("🚀 Starting CSV Processing and Chunking")
    print("=" * 50)
    
    # Step 1: Choose processing method
    print("\nChoose processing method:")
    print("1. Standard batched processing (recommended)")
    print("2. Streaming processing (for very large files)")
    print("3. Create sample files only")
    
    choice = input("Enter your choice (1/2/3): ").strip()
    
    if choice == '3':
        create_sample_files()
        exit()
    
    # Step 2: Configure files for chunking
    files_to_process = [
        {
            'file': 'age_data.csv',
            'chunk_size': 1000000,  # 1M rows per chunk
            'output_dir': 'age_data_chunks'
        },
        {
            'file': 'source_data.csv',
            'chunk_size': 1000000,  # 1M rows per chunk  
            'output_dir': 'source_data_chunks'
        },
        {
            'file': 'chocolate_consumption_data.csv',
            'chunk_size': 1000000,  # 1M rows per chunk  
            'output_dir': 'source_data_chunks'
        }
    ]
    
    # Step 3: Process all files
    print(f"\n📋 Files configured for processing:")
    for config in files_to_process:
        if os.path.exists(config['file']):
            print(f"  ✅ {config['file']} -> {config['chunk_size']:,} rows per chunk")
        else:
            print(f"  ❌ {config['file']} -> FILE NOT FOUND")
    
    proceed = input("\nProceed with chunking? (y/n): ").lower().strip()
    if proceed == 'y':
        if choice == '2':
            # Use streaming approach
            results = {}
            for config in files_to_process:
                if os.path.exists(config['file']):
                    print(f"\n{'='*60}")
                    print(f"Streaming processing: {config['file']}")
                    print(f"{'='*60}")
                    
                    summary = chunk_csv_file_streaming(
                        input_file=config['file'],
                        chunk_size=config['chunk_size'],
                        output_dir=config['output_dir']
                    )
                    
                    if summary:
                        results[config['file']] = summary
        else:
            # Use standard approach
            results = process_multiple_files(files_to_process)
        
        # Final summary
        print(f"\n🏁 FINAL SUMMARY")
        print(f"=" * 50)
        for file, summary in results.items():
            print(f"📄 {file}:")
            print(f"   Total rows: {summary['total_rows']:,}")
            print(f"   Chunks created: {summary['num_chunks']}")
            print(f"   Output directory: {summary['output_directory']}")
            print()
    else:
        print("Chunking cancelled.")