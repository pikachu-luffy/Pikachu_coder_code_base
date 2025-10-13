import csv

def extract_apple_idfa_from_csv(file_path):
    """
    Extract all apple_id_for_advertisers_idfa values from the CSV file
    
    Args:
        file_path (str): Path to the CSV file
        
    Returns:
        list: List of all apple_id_for_advertisers_idfa values
    """
    apple_idfa_list = []
    
    try:
        with open(file_path, 'r', newline='', encoding='utf-8') as csvfile:
            # Create CSV reader
            csv_reader = csv.DictReader(csvfile)
            
            # Extract apple_id_for_advertisers_idfa from each row
            for row in csv_reader:
                idfa_value = row.get('apple_id_for_advertisers_idfa', '').strip()
                if idfa_value:  # Only add non-empty values
                    apple_idfa_list.append(idfa_value)
        
        print(f"Successfully extracted {len(apple_idfa_list)} Apple IDFA values")
        return apple_idfa_list
        
    except FileNotFoundError:
        print(f"Error: File '{file_path}' not found")
        return []
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return []

def extract_apple_idfa_alternative(file_path):
    """
    Alternative method using column index instead of column name
    
    Args:
        file_path (str): Path to the CSV file
        
    Returns:
        list: List of all apple_id_for_advertisers_idfa values
    """
    apple_idfa_list = []
    
    try:
        with open(file_path, 'r', newline='', encoding='utf-8') as csvfile:
            csv_reader = csv.reader(csvfile)
            
            # Read header to find the column index
            header = next(csv_reader)
            try:
                idfa_column_index = header.index('apple_id_for_advertisers_idfa')
            except ValueError:
                print("Error: 'apple_id_for_advertisers_idfa' column not found in CSV")
                return []
            
            # Extract values from the specific column
            for row in csv_reader:
                if len(row) > idfa_column_index:
                    idfa_value = row[idfa_column_index].strip()
                    if idfa_value:  # Only add non-empty values
                        apple_idfa_list.append(idfa_value)
        
        print(f"Successfully extracted {len(apple_idfa_list)} Apple IDFA values")
        return apple_idfa_list
        
    except FileNotFoundError:
        print(f"Error: File '{file_path}' not found")
        return []
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return []

def main():
    # File path
    csv_file_path = "dummy_data_with_hashes.csv"
    
    print("=" * 60)
    print("EXTRACTING APPLE IDFA VALUES FROM CSV")
    print("=" * 60)
    
    # Method 1: Using DictReader (recommended)
    print("\nMethod 1: Using DictReader")
    apple_idfa_list = extract_apple_idfa_from_csv(csv_file_path)
    
    # Display first 10 values as sample
    if apple_idfa_list:
        print(f"\nFirst 10 Apple IDFA values:")
        for i, idfa in enumerate(apple_idfa_list[:10], 1):
            print(f"{i:2d}. {idfa}")
        
        if len(apple_idfa_list) > 10:
            print(f"... and {len(apple_idfa_list) - 10} more values")
        
        print(f"\nTotal Apple IDFA values extracted: {len(apple_idfa_list)}")
        
        # Save to a text file for reference
        output_file = "extracted_apple_idfa_list.txt"
        with open(output_file, 'w') as f:
            f.write("Apple IDFA Values:\n")
            f.write("=" * 50 + "\n")
            for i, idfa in enumerate(apple_idfa_list, 1):
                f.write(f"{i}. {idfa}\n")
        
        print(f"Apple IDFA list saved to: {output_file}")
        
        # Return the list for further use
        return apple_idfa_list
    else:
        print("No Apple IDFA values found or error occurred")
        return []

# Alternative function to get unique IDFA values
def get_unique_apple_idfa(file_path):
    """
    Extract unique apple_id_for_advertisers_idfa values from the CSV file
    
    Args:
        file_path (str): Path to the CSV file
        
    Returns:
        list: List of unique apple_id_for_advertisers_idfa values
    """
    apple_idfa_set = set()
    
    try:
        with open(file_path, 'r', newline='', encoding='utf-8') as csvfile:
            csv_reader = csv.DictReader(csvfile)
            
            for row in csv_reader:
                idfa_value = row.get('apple_id_for_advertisers_idfa', '').strip()
                if idfa_value:
                    apple_idfa_set.add(idfa_value)
        
        unique_idfa_list = list(apple_idfa_set)
        print(f"Found {len(unique_idfa_list)} unique Apple IDFA values")
        return unique_idfa_list
        
    except Exception as e:
        print(f"Error: {e}")
        return []

if __name__ == "__main__":
    # Extract all Apple IDFA values
    idfa_list = main()
    
    # Optional: Get unique values only
    print("\n" + "=" * 60)
    print("EXTRACTING UNIQUE APPLE IDFA VALUES")
    print("=" * 60)
    unique_idfa_list = get_unique_apple_idfa("dummy_data_with_hashes.csv")
    
    if unique_idfa_list:
        print(f"\nFirst 5 unique Apple IDFA values:")
        for i, idfa in enumerate(unique_idfa_list[:5], 1):
            print(f"{i}. {idfa}")