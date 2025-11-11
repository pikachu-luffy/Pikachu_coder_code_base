import csv
import os
import math
from pathlib import Path

def split_csv_into_files(input_file: str, num_files: int = 20, output_dir: str = None):
    """
    Split a CSV file into a specified number of smaller files using pure Python.
    
    Args:
        input_file (str): Path to the input CSV file
        num_files (int): Number of output files to create (default: 20)
        output_dir (str): Directory to save split files (auto-generated if None)
    
    Returns:
        dict: Summary information about the splitting process
    """
    
    # Validate input file exists
    if not os.path.exists(input_file):
        print(f"❌ Error: Input file '{input_file}' not found!")
        return None
    
    # Auto-generate output directory name if not provided
    if output_dir is None:
        base_name = os.path.splitext(os.path.basename(input_file))[0]
        output_dir = f"{base_name}_split_{num_files}_files"
    
    # Create output directory if it doesn't exist
    Path(output_dir).mkdir(exist_ok=True)
    
    print(f"\n🚀 Starting to split file: {input_file}")
    print(f"📁 Output directory: {output_dir}")
    print(f"📊 Target number of files: {num_files}")
    
    # Step 1: Count total rows in the file
    print("\n📏 Counting total rows...")
    total_rows = 0
    header_row = None
    
    try:
        with open(input_file, 'r', encoding='utf-8', newline='') as file:
            csv_reader = csv.reader(file)
            header_row = next(csv_reader)  # Read header
            
            # Count data rows (excluding header)
            for row in csv_reader:
                if row:  # Skip empty rows
                    total_rows += 1
        
        print(f"✅ Total data rows (excluding header): {total_rows:,}")
        print(f"📋 Header: {header_row[:5]}..." if len(header_row) > 5 else f"📋 Header: {header_row}")
        
    except Exception as e:
        print(f"❌ Error reading file: {e}")
        return None
    
    if total_rows == 0:
        print("❌ No data rows found in the file!")
        return None
    
    # Step 2: Calculate rows per file
    rows_per_file = math.ceil(total_rows / num_files)
    print(f"📐 Rows per file: {rows_per_file:,}")
    
    # Step 3: Split the file
    print(f"\n✂️ Splitting file into {num_files} parts...")
    
    base_filename = os.path.splitext(os.path.basename(input_file))[0]
    file_info = []
    current_file_num = 1
    current_row_count = 0
    current_file = None
    current_writer = None
    
    try:
        with open(input_file, 'r', encoding='utf-8', newline='') as input_file_handle:
            csv_reader = csv.reader(input_file_handle)
            next(csv_reader)  # Skip header in input (we'll add it to each output file)
            
            for row_num, row in enumerate(csv_reader, 1):
                if not row:  # Skip empty rows
                    continue
                
                # Open new file if needed
                if current_file is None or current_row_count >= rows_per_file:
                    # Close previous file if open
                    if current_file is not None:
                        current_file.close()
                        print(f"✅ Completed file {current_file_num-1}: {current_row_count:,} rows")
                    
                    # Don't create more files than requested
                    if current_file_num > num_files:
                        # Add remaining rows to the last file
                        current_file_num = num_files
                        output_filename = os.path.join(output_dir, f"{base_filename}_part_{current_file_num:02d}.csv")
                        current_file = open(output_filename, 'a', encoding='utf-8', newline='')
                        current_writer = csv.writer(current_file)
                    else:
                        # Create new file
                        output_filename = os.path.join(output_dir, f"{base_filename}_part_{current_file_num:02d}.csv")
                        current_file = open(output_filename, 'w', encoding='utf-8', newline='')
                        current_writer = csv.writer(current_file)
                        
                        # Write header to new file
                        current_writer.writerow(header_row)
                        
                        # Track file info
                        file_info.append({
                            'file_number': current_file_num,
                            'filename': output_filename,
                            'start_row': row_num,
                            'row_count': 0
                        })
                        
                        current_row_count = 0
                        current_file_num += 1
                
                # Write row to current file
                current_writer.writerow(row)
                current_row_count += 1
                file_info[-1]['row_count'] += 1
                file_info[-1]['end_row'] = row_num
                
                # Progress indicator
                if row_num % 100000 == 0:
                    print(f"📝 Processed {row_num:,} rows...")
        
        # Close the last file
        if current_file is not None:
            current_file.close()
            print(f"✅ Completed file {len(file_info)}: {current_row_count:,} rows")
    
    except Exception as e:
        print(f"❌ Error during file splitting: {e}")
        if current_file is not None:
            current_file.close()
        return None
    
    # Step 4: Verify and create summary
    print(f"\n🔍 Verification:")
    total_processed_rows = sum(info['row_count'] for info in file_info)
    
    if total_processed_rows == total_rows:
        print(f"✅ SUCCESS: All {total_rows:,} rows have been split into {len(file_info)} files")
    else:
        print(f"❌ WARNING: Expected {total_rows:,} rows but processed {total_processed_rows:,} rows")
    
    # Create detailed summary
    summary = {
        'input_file': input_file,
        'total_rows': total_rows,
        'processed_rows': total_processed_rows,
        'target_files': num_files,
        'actual_files': len(file_info),
        'rows_per_file': rows_per_file,
        'output_directory': output_dir,
        'header': header_row,
        'files': file_info
    }
    
    # Display file details
    print(f"\n📋 File Details:")
    print(f"{'File':<15} {'Filename':<30} {'Rows':<10} {'Range':<20}")
    print("-" * 75)
    
    for info in file_info:
        filename = os.path.basename(info['filename'])
        row_range = f"{info['start_row']}-{info['end_row']}"
        print(f"{info['file_number']:<15} {filename:<30} {info['row_count']:<10,} {row_range:<20}")
    
    # Save summary to file
    base_filename = os.path.splitext(os.path.basename(input_file))[0]
    summary_file = os.path.join(output_dir, f"{base_filename}_split_summary.txt")
    
    with open(summary_file, 'w', encoding='utf-8') as f:
        f.write(f"CSV File Split Summary\n")
        f.write(f"=" * 50 + "\n\n")
        f.write(f"Input file: {input_file}\n")
        f.write(f"Total data rows: {total_rows:,}\n")
        f.write(f"Processed rows: {total_processed_rows:,}\n")
        f.write(f"Target files: {num_files}\n")
        f.write(f"Actual files created: {len(file_info)}\n")
        f.write(f"Rows per file (target): {rows_per_file:,}\n")
        f.write(f"Output directory: {output_dir}\n")
        f.write(f"Header columns: {len(header_row)}\n\n")
        
        f.write(f"File Details:\n")
        f.write(f"-" * 80 + "\n")
        f.write(f"{'File':<6} {'Filename':<35} {'Rows':<10} {'Row Range':<15}\n")
        f.write(f"-" * 80 + "\n")
        
        for info in file_info:
            filename = os.path.basename(info['filename'])
            row_range = f"{info['start_row']}-{info['end_row']}"
            f.write(f"{info['file_number']:<6} {filename:<35} {info['row_count']:<10,} {row_range:<15}\n")
    
    print(f"\n📄 Summary saved to: {summary_file}")
    
    return summary

def verify_split_files(output_dir: str, original_total_rows: int):
    """
    Verify that all split files contain the expected data.
    
    Args:
        output_dir (str): Directory containing split files
        original_total_rows (int): Expected total number of data rows
    """
    print(f"\n🔍 Verifying split files in: {output_dir}")
    
    csv_files = sorted([f for f in os.listdir(output_dir) if f.endswith('.csv')])
    
    if not csv_files:
        print("❌ No CSV files found in output directory!")
        return False
    
    total_verified_rows = 0
    header_consistency = True
    first_header = None
    
    for i, csv_file in enumerate(csv_files, 1):
        file_path = os.path.join(output_dir, csv_file)
        
        try:
            with open(file_path, 'r', encoding='utf-8', newline='') as f:
                csv_reader = csv.reader(f)
                header = next(csv_reader)
                
                # Check header consistency
                if first_header is None:
                    first_header = header
                elif header != first_header:
                    print(f"❌ Header mismatch in {csv_file}")
                    header_consistency = False
                
                # Count data rows
                row_count = sum(1 for row in csv_reader if row)
                total_verified_rows += row_count
                
                print(f"✅ {csv_file}: {row_count:,} data rows")
                
        except Exception as e:
            print(f"❌ Error reading {csv_file}: {e}")
            return False
    
    print(f"\n📊 Verification Results:")
    print(f"   Files checked: {len(csv_files)}")
    print(f"   Total data rows: {total_verified_rows:,}")
    print(f"   Expected rows: {original_total_rows:,}")
    print(f"   Header consistency: {'✅ Good' if header_consistency else '❌ Issues found'}")
    
    if total_verified_rows == original_total_rows and header_consistency:
        print(f"🎉 All files verified successfully!")
        return True
    else:
        print(f"❌ Verification failed!")
        return False

# Main execution function
def main():
    """Main function to split the age_data.csv file."""
    
    print("🚀 CSV File Splitter (Pure Python)")
    print("=" * 50)
    
    # Configuration
    # input_file = "age_data.csv"
    # input_file = "source_data.csv"
    # input_file = "chocolate_consumption_data.csv"
    input_file = "parent_flag_data.csv"
    num_files = 100
    # output_dir = "age_data_100_files"
    # output_dir = "source_data_100_files"
    output_dir = "parent_flag_data_100_files"
    
    # Check if input file exists
    if not os.path.exists(input_file):
        print(f"❌ Error: '{input_file}' not found in current directory!")
        print("Please make sure the file exists and try again.")
        return
    
    print(f"📄 Input file: {input_file}")
    print(f"🎯 Target: {num_files} output files")
    print(f"📁 Output directory: {output_dir}")
    
    # Ask for confirmation
    proceed = input(f"\nProceed with splitting '{input_file}' into {num_files} files? (y/n): ").lower().strip()
    
    if proceed != 'y':
        print("Operation cancelled.")
        return
    
    # Split the file
    summary = split_csv_into_files(
        input_file=input_file,
        num_files=num_files,
        output_dir=output_dir
    )
    
    if summary is None:
        print("❌ File splitting failed!")
        return
    
    # Verify the results
    verification_success = verify_split_files(output_dir, summary['total_rows'])
    
    # Final summary
    print(f"\n🏁 FINAL RESULTS")
    print(f"=" * 50)
    print(f"✅ Input file: {summary['input_file']}")
    print(f"✅ Total rows processed: {summary['processed_rows']:,}")
    print(f"✅ Files created: {summary['actual_files']}")
    print(f"✅ Output directory: {summary['output_directory']}")
    print(f"✅ Verification: {'Passed' if verification_success else 'Failed'}")
    
    if verification_success:
        print(f"\n🎉 SUCCESS! Your CSV file has been split into {summary['actual_files']} files.")
        print(f"📁 Check the '{output_dir}' directory for your files.")
    else:
        print(f"\n⚠️ Splitting completed but verification found issues.")

if __name__ == "__main__":
    main()