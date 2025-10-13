import hashlib
import csv
import random
import base64
from datetime import datetime, timedelta
from dateutil.tz import gettz
from cryptography.fernet import Fernet

def generate_dummy_data():
    """Generate dummy data CSV with phone numbers, emails and their encrypted versions according to new schema"""
    
    # Generate encryption key (save this for decryption!)
    encryption_key = Fernet.generate_key()
    cipher_suite = Fernet(encryption_key)
    
    # Print the key for future decryption use
    print("=" * 60)
    print("ENCRYPTION KEY (SAVE THIS FOR DECRYPTION):")
    print(f"Key: {encryption_key.decode()}")
    print("=" * 60)
    
    # Phone numbers provided
    phone_numbers = [
        "9873185132", "9997297420", "8090780377", "7017890375", "7409898048",
        "7906063900", "7906310216", "9927173568", "8046396150", "8851872499",
        "7619977823", "9958446833", "8460779976", "8035059316", "9458744602",
        "8390690180", "8047943528", "9627363411", "7505720414", "9228833947", 
        "9178494050", "8279679317", "9422330972", "7016294998", "5678456012", 
        "4444333351", "5901234567", "5555666666", "1239087651"
    ]
    
    # Email addresses provided
    email_addresses = [
        "jokerrobbat@gmail.com", "narutouzumakichauhan1995@gmail.com", 
        "pikachuashchut@gmail.com", "pikachuashchut5@gmail.com", 
        "karnamohanta7@gmail.com", "shubashkumar2021@gmail.com", 
        "sanketnigudkar@gmail.com", "sisodiyapradipsinh2@gmail.com", 
        "roronoazorochauhan1995@gmail.com", "gonkilluachauhan1995@gmail.com", 
        "monkeydluffychauhan1995@gmail.com"
    ]
    
    # Extra columns for testing error columns phenomena
    nick_names = ["teenage", "old", "newborn", "mildborn"]
    religion_names = ['hindu', 'christian', 'jain', 'buddhist']
    consent_values = ['0', '1']
    
    # Sample data pools for generating realistic dummy data
    first_names = ["Rahul", "Priya", "Amit", "Sneha", "Vikram", "Anita", "Rajesh", "Kavya", "Suresh", "Meera", 
                   "Arjun", "Pooja", "Kiran", "Deepika", "Manoj", "Ritu", "Sanjay", "Nisha"]
    
    last_names = ["Sharma", "Patel", "Singh", "Kumar", "Gupta", "Agarwal", "Jain", "Verma", "Shah", "Mehta",
                  "Reddy", "Nair", "Iyer", "Chopra", "Malhotra", "Bansal", "Saxena", "Tiwari"]
    
    cities = ["Mumbai", "Delhi", "Bangalore", "Chennai", "Kolkata", "Hyderabad", "Pune", "Ahmedabad", 
              "Jaipur", "Lucknow", "Kanpur", "Nagpur", "Indore", "Bhopal", "Visakhapatnam", "Patna"]
    
    states = ["Maharashtra", "Delhi", "Karnataka", "Tamil Nadu", "West Bengal", "Telangana", "Gujarat", 
              "Rajasthan", "Uttar Pradesh", "Madhya Pradesh", "Andhra Pradesh", "Bihar", "Kerala", "Punjab"]
    
    genders = ["Male", "Female", "Other"]
    sources = ["Website", "Mobile App", "Social Media", "Email Campaign", "Referral", "Advertisement"]
    
    brands = ["Cadbury", "Oreo", "Bournvita", "5Star", "Gems", "Dairy Milk", "Silk"]
    frequencies = ["Daily", "Weekly", "Monthly", "Occasionally", "Rarely"]
    bool_values = [True, False]  # For BOOL type columns
    preferences = ["Sweet", "Bitter", "Milk Chocolate", "Dark Chocolate", "White Chocolate"]
    liked_recipes = ["Chocolate Cake", "Brownies", "Cookies", "Ice Cream", "Pudding"]
    isscan_values = ["Yes", "No"]
    packtype_values = ["Pouch", "Box", "Wrapper", "Bottle", "Can"]
    created_story_values = ["Adventure", "Romance", "Mystery", "Comedy", "Drama"]
    cricket_answers = ["Batting", "Bowling", "Fielding", "All-rounder", "Captain"]
    coupon_flag_values = ["Yes", "No"]
    
    # CSV header with encrypted columns added
    header = [
        "first_name", "last_name", "phone", "email", "encrypted_phone", "encrypted_email",
        "google_advertiser_id_gaid", "apple_id_for_advertisers_idfa",
        "consent_timestamp", "date_of_birth", "city", "gender", "state", "source", 
        "what_is_your_favourite_brand", "how_often_do_you_consume_biscuits", 
        "are_you_a_parent", "liked_recipes", "isscan", "zip", "packtype", 
        "chocolate_consumption", "created_story", "jio_gems_cricket_answer", 
        "cadbury_brand", "mithai_consumption_flag", "candies_consumption_flag", 
        "biscuit_consumption_flag", "birth_year", "coupon_flag",
        "nick_name", "religion"
    ]
    
    # Generate data rows with different combinations
    rows = []
    
    # Create a larger pool by combining phone numbers and emails
    # We'll create different combinations: phone only, email only, both, encrypted_only
    max_records = max(len(phone_numbers), len(email_addresses)) * 3  # Generate more records
    
    for i in range(max_records):
        # Determine what type of record to create
        # Added 'encrypted_only' for records with only encrypted data
        record_type = random.choice(['phone_only', 'email_only', 'both', 'encrypted_only'])
        
        # Initialize all variables
        phone = ""
        email = ""
        encrypted_phone = ""
        encrypted_email = ""
        
        if record_type == 'phone_only':
            # Only phone, no email, no encrypted data
            phone = random.choice(phone_numbers)
            email = ""
            encrypted_phone = ""
            encrypted_email = ""
            
        elif record_type == 'email_only':
            # Only email, no phone, no encrypted data
            email = random.choice(email_addresses)
            phone = ""
            encrypted_phone = ""
            encrypted_email = ""
            
        elif record_type == 'both':
            # Both phone and email, no encrypted data
            phone = random.choice(phone_numbers)
            email = random.choice(email_addresses)
            encrypted_phone = ""
            encrypted_email = ""
            
        else:  # encrypted_only
            # No plain phone/email, only encrypted versions
            phone = ""
            email = ""
            
            # Encrypt phone and email data
            phone_to_encrypt = random.choice(phone_numbers)
            email_to_encrypt = random.choice(email_addresses)
            
            # Encrypt the data
            encrypted_phone = cipher_suite.encrypt(phone_to_encrypt.encode()).decode()
            encrypted_email = cipher_suite.encrypt(email_to_encrypt.encode()).decode()
        
        # Extra columns for testing
        nick_name = random.choice(nick_names)
        religion = random.choice(religion_names)
        consent_data = random.choice(consent_values)
        
        # Generate proper timestamp format for TIMESTAMP type
        consent_timestamp = datetime.now(tz=gettz('UTC')).strftime('%Y-%m-%d %H:%M:%S UTC')
        
        # Generate other dummy data
        first_name = random.choice(first_names)
        last_name = random.choice(last_names)
        
        # Generate birth year (age 18-65) as INT64
        birth_year = random.randint(1959, 2006)  # Current year 2024 - 65 to 2024 - 18
        
        # Generate proper date format for DATE type (YYYY-MM-DD)
        birth_month = random.randint(1, 12)
        birth_day = random.randint(1, 28)  # Safe day range for all months
        date_of_birth = f"{birth_year}-{birth_month:02d}-{birth_day:02d}"
        
        # Generate random zip code (6 digits)
        zip_code = f"{random.randint(100000, 999999)}"
        
        # Generate random GAID and IDFA (UUID-like format)
        gaid = f"{random.randint(10000000, 99999999)}-{random.randint(1000, 9999)}-{random.randint(1000, 9999)}-{random.randint(1000, 9999)}-{random.randint(100000000000, 999999999999)}"
        idfa = f"{random.randint(10000000, 99999999)}-{random.randint(1000, 9999)}-{random.randint(1000, 9999)}-{random.randint(1000, 9999)}-{random.randint(100000000000, 999999999999)}"
        
        row = [
            first_name,
            last_name,
            phone,  # Phone column (can be empty)
            email,  # Email column (can be empty)
            encrypted_phone,  # Encrypted phone (can be empty)
            encrypted_email,  # Encrypted email (can be empty)
            gaid,  # google_advertiser_id__gaid_
            idfa,  # apple_id_for_advertisers__idfa_
            consent_timestamp,
            date_of_birth,  # DATE format
            random.choice(cities),
            random.choice(genders),
            random.choice(states),
            random.choice(sources),
            random.choice(brands),  # what_is_your_favourite_brand_
            random.choice(frequencies),  # how_often_do_you_consume_biscuits_
            random.choice(bool_values),  # are_you_a_parent (BOOL)
            random.choice(liked_recipes),
            random.choice(isscan_values),
            zip_code,
            random.choice(packtype_values),
            random.choice(frequencies),  # chocolate_consumption
            random.choice(created_story_values),
            random.choice(cricket_answers),  # jio_gems_cricket_answer
            random.choice(preferences),  # cadbury_brand
            random.choice(bool_values),  # mithai_consumption_flag (BOOL)
            random.choice(bool_values),  # candies_consumption_flag (BOOL)
            random.choice(bool_values),  # biscuit_consumption_flag (BOOL)
            birth_year,  # INT64
            random.choice(coupon_flag_values),
            nick_name,  # Extra column for testing
            religion   # Extra column for testing
        ]
        
        rows.append(row)
    
    # Write to CSV file
    output_file = "dummy_data_with_encryption_try_1.csv"
    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(header)
        writer.writerows(rows)
    
    # Save encryption key to a separate file for safekeeping
    key_file = "encryption_key.txt"
    with open(key_file, 'w') as f:
        f.write(f"Encryption Key: {encryption_key.decode()}\n")
        f.write(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("Use this key to decrypt encrypted_phone and encrypted_email columns\n")
    
    # Count different record types for reporting
    phone_only_count = sum(1 for row in rows if row[2] and not row[3] and not row[4] and not row[5])  # phone only
    email_only_count = sum(1 for row in rows if row[3] and not row[2] and not row[4] and not row[5])  # email only
    both_count = sum(1 for row in rows if row[2] and row[3] and not row[4] and not row[5])  # both phone and email
    encrypted_only_count = sum(1 for row in rows if not row[2] and not row[3] and row[4] and row[5])  # encrypted only
    
    print(f"\nGenerated {len(rows)} rows of dummy data in '{output_file}'")
    print(f"Record distribution:")
    print(f"  - Phone only: {phone_only_count} records")
    print(f"  - Email only: {email_only_count} records")
    print(f"  - Both phone and email: {both_count} records")
    print(f"  - Encrypted only: {encrypted_only_count} records")
    print(f"\nEncryption key saved to: {key_file}")
    print(f"Sample phone number hash: {phone_numbers[0]} -> {hashlib.sha256(phone_numbers[0].encode('utf-8')).hexdigest()}")
    print(f"Sample email hash: {email_addresses[0]} -> {hashlib.sha256(email_addresses[0].encode('utf-8')).hexdigest()}")
    
    # Show sample decryption
    if encrypted_only_count > 0:
        sample_encrypted_row = next(row for row in rows if row[4] and row[5])
        print(f"\nSample encrypted data:")
        print(f"  - Encrypted phone: {sample_encrypted_row[4][:50]}...")
        print(f"  - Encrypted email: {sample_encrypted_row[5][:50]}...")
        
        # Decrypt sample to verify
        try:
            decrypted_phone = cipher_suite.decrypt(sample_encrypted_row[4].encode()).decode()
            decrypted_email = cipher_suite.decrypt(sample_encrypted_row[5].encode()).decode()
            print(f"  - Decrypted phone: {decrypted_phone}")
            print(f"  - Decrypted email: {decrypted_email}")
        except Exception as e:
            print(f"  - Decryption test failed: {e}")
    
    print("\nSchema updates applied:")
    print("- Added encrypted_phone and encrypted_email columns")
    print("- Added 'encrypted_only' record type with no plain text data")
    print("- Plain text and encrypted data are mutually exclusive")
    print("- Used Fernet encryption from cryptography library")
    print("- Encryption key saved for future decryption needs")
    print("- Maintained consistent column count across all record types")
    
    return output_file, encryption_key.decode()

def decrypt_data(encrypted_text, key):
    """Helper function to decrypt data using the provided key"""
    try:
        cipher_suite = Fernet(key.encode())
        decrypted_text = cipher_suite.decrypt(encrypted_text.encode()).decode()
        return decrypted_text
    except Exception as e:
        return f"Decryption failed: {e}"

if __name__ == "__main__":
    output_file, key = generate_dummy_data()
    print(f"\n{'='*60}")
    print("IMPORTANT: Save the encryption key for decryption!")
    print(f"Key: {key}")
    print(f"{'='*60}")