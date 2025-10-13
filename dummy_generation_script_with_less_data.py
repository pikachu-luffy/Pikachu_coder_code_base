import hashlib
import csv
import random
import re
from datetime import datetime, timedelta
from dateutil.tz import gettz

def normalize_phone(phone):
    """
    Normalize phone number by adding +91 prefix
    
    Args:
        phone (str): Original phone number
        
    Returns:
        str: Normalized phone number with +91 prefix
    """
    return f"+91{phone}"

def normalize_email(email):
    """
    Normalize email by removing special characters (., _, etc.) before domain
    
    Args:
        email (str): Original email address
        
    Returns:
        str: Normalized email address
    """
    if '@' not in email:
        return email
    
    local_part, domain = email.split('@', 1)
    
    # Remove special characters from local part (before @)
    # Keep only alphanumeric characters
    normalized_local = re.sub(r'[^a-zA-Z0-9]', '', local_part)
    
    return f"{normalized_local}@{domain}"

def generate_dummy_data():
    """Generate dummy data CSV with phone numbers, emails and their SHA256 hashes according to new schema"""
    
    # Phone numbers provided
    phone_numbers = [
        "9873185132", "9997297420", "8090780377", "7017890375", "7409898048",
        "7906063900", "7906310216", "9927173568", "8046396150", "8851872499",
        "7619977823", "9958446833", "8460779976", "8035059316", "9458744602",
        "8390690180", "8047943528", "9627363411", "7505720414", "9228833947", 
        "9178494050", "8279679317", "9422330972", "7016294998", "5678456012", 
        "4444333351", "5901234567", "5555666666", "1239087651", "8171014733"
    ]
    
    # Email addresses provided
    email_addresses = [
        "jokerrobbat@gmail.com", "narutouzumakichauhan1995@gmail.com", 
        "pikachuashchut@gmail.com", "pikachuashchut5@gmail.com", 
        "karnamohanta7@gmail.com", "shubashkumar2021@gmail.com", 
        "sanketnigudkar@gmail.com", "sisodiyapradipsinh2@gmail.com", 
        "roronoazorochauhan1995@gmail.com", "gonkilluachauhan1995@gmail.com", 
        "monkeydluffychauhan1995@gmail.com", "priyank.chauhan0@gmail.com"
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
    
    # CSV header with added hash columns
    # header = [
    #     "first_name", "last_name", "phone", "email", "phone_hashed", "email_hashed",
    #     "google_advertiser_id_gaid", "apple_id_for_advertisers_idfa",
    #     "consent_timestamp", "date_of_birth", "city", "gender", "state", "source", 
    #     "what_is_your_favourite_brand", "how_often_do_you_consume_biscuits", 
    #     "are_you_a_parent", "liked_recipes", "isscan", "zip", "packtype", 
    #     "chocolate_consumption", "created_story","cadbury_brand",
    #     "mithai_consumption_flag", "candies_consumption_flag", 
    #     "biscuit_consumption_flag", "birth_year", "coupon_flag"
    # ]
    
    header = [
        "first_name", "last_name", "phone", "email", "phone_hashed", "email_hashed",
        "consent_timestamp", "date_of_birth", "city", "gender", "state", "source", 
        "what_is_your_favourite_brand", "how_often_do_you_consume_biscuits", 
        "are_you_a_parent", "liked_recipes", "isscan", "zip", "packtype", 
        "chocolate_consumption", "created_story","cadbury_brand",
        "mithai_consumption_flag", "candies_consumption_flag", 
        "biscuit_consumption_flag", "birth_year", "coupon_flag"
    ]
    # Generate data rows with different combinations
    rows = []
    
    # Create a larger pool by combining phone numbers and emails
    # We'll create different combinations: phone only, email only, both, hashed_only, normalized_phone_hash_only, normalized_email_hash_only
    max_records = max(len(phone_numbers), len(email_addresses)) * 5  # Generate more records
    
    for i in range(max_records):
        # Determine what type of record to create
        # Added new record types for normalized hash-only data
        record_type = random.choice([
            'phone_only', 'email_only', 'both', 'hashed_only', 
            'normalized_phone_hash_only', 'normalized_email_hash_only'
        ])
        
        # Initialize all variables
        phone = ""
        email = ""
        phone_hashed = ""
        email_hashed = ""
        
        if record_type == 'phone_only':
            # Only phone, no email, with corresponding normalized hash
            phone = random.choice(phone_numbers)
            normalized_phone = normalize_phone(phone)
            phone_hashed = hashlib.sha256(normalized_phone.encode('utf-8')).hexdigest()
            email = ""
            email_hashed = ""
            
        elif record_type == 'email_only':
            # Only email, no phone, with corresponding normalized hash
            email = random.choice(email_addresses)
            normalized_email = normalize_email(email)
            email_hashed = hashlib.sha256(normalized_email.encode('utf-8')).hexdigest()
            phone = ""
            phone_hashed = ""
            
        elif record_type == 'both':
            # Both phone and email with their corresponding normalized hashes
            phone = random.choice(phone_numbers)
            email = random.choice(email_addresses)
            normalized_phone = normalize_phone(phone)
            normalized_email = normalize_email(email)
            phone_hashed = hashlib.sha256(normalized_phone.encode('utf-8')).hexdigest()
            email_hashed = hashlib.sha256(normalized_email.encode('utf-8')).hexdigest()
            
        elif record_type == 'hashed_only':
            # No plain phone/email, only normalized hashed versions (vendor sends only hashes)
            phone = ""
            email = ""
            
            # Generate normalized hashes from original data but don't store the plain text
            phone_to_hash = random.choice(phone_numbers)
            email_to_hash = random.choice(email_addresses)
            
            normalized_phone = normalize_phone(phone_to_hash)
            normalized_email = normalize_email(email_to_hash)
            
            phone_hashed = hashlib.sha256(normalized_phone.encode('utf-8')).hexdigest()
            email_hashed = hashlib.sha256(normalized_email.encode('utf-8')).hexdigest()
            
        elif record_type == 'normalized_phone_hash_only':
            # Only normalized phone hash, no other phone/email data
            phone = ""
            email = ""
            email_hashed = ""
            
            phone_to_hash = random.choice(phone_numbers)
            normalized_phone = normalize_phone(phone_to_hash)
            phone_hashed = hashlib.sha256(normalized_phone.encode('utf-8')).hexdigest()
            
        else:  # normalized_email_hash_only
            # Only normalized email hash, no other phone/email data
            phone = ""
            email = ""
            phone_hashed = ""
            
            email_to_hash = random.choice(email_addresses)
            normalized_email = normalize_email(email_to_hash)
            email_hashed = hashlib.sha256(normalized_email.encode('utf-8')).hexdigest()
        
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
            phone_hashed,  # Phone hash (can be empty) - based on normalized phone
            email_hashed,  # Email hash (can be empty) - based on normalized email
            # gaid,  # google_advertiser_id_gaid
            # idfa,  # apple_id_for_advertisers_idfa
            consent_timestamp,
            date_of_birth,  # DATE format
            random.choice(cities),
            random.choice(genders),
            random.choice(states),
            random.choice(sources),
            random.choice(brands),  # what_is_your_favourite_brand
            random.choice(frequencies),  # how_often_do_you_consume_biscuits
            random.choice(bool_values),  # are_you_a_parent (BOOL)
            random.choice(liked_recipes),
            random.choice(isscan_values),
            zip_code,
            random.choice(packtype_values),
            random.choice(frequencies),  # chocolate_consumption
            random.choice(created_story_values),
            # random.choice(cricket_answers),  # jio_gems_cricket_answer
            random.choice(preferences),  # cadbury_brand
            random.choice(bool_values),  # mithai_consumption_flag (BOOL)
            random.choice(bool_values),  # candies_consumption_flag (BOOL)
            random.choice(bool_values),  # biscuit_consumption_flag (BOOL)
            birth_year,  # INT64
            random.choice(coupon_flag_values),
        ]
        
        rows.append(row)
    
    # Write to CSV file
    output_file = "dummy_data_with_normalized_hashes_paytm.csv"
    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(header)
        writer.writerows(rows)
    
    # Count different record types for reporting
    phone_only_count = sum(1 for row in rows if row[2] and not row[3] and row[4] and not row[5])  # phone + phone_hash only
    email_only_count = sum(1 for row in rows if row[3] and not row[2] and row[5] and not row[4])  # email + email_hash only
    both_count = sum(1 for row in rows if row[2] and row[3] and row[4] and row[5])  # both phone/email + both hashes
    hashed_only_count = sum(1 for row in rows if not row[2] and not row[3] and row[4] and row[5])  # only hashes, no plain text
    normalized_phone_hash_only_count = sum(1 for row in rows if not row[2] and not row[3] and row[4] and not row[5])  # only normalized phone hash
    normalized_email_hash_only_count = sum(1 for row in rows if not row[2] and not row[3] and not row[4] and row[5])  # only normalized email hash
    
    print(f"Generated {len(rows)} rows of dummy data in '{output_file}'")
    print(f"Record distribution:")
    print(f"  - Phone only (with normalized hash): {phone_only_count} records")
    print(f"  - Email only (with normalized hash): {email_only_count} records")
    print(f"  - Both phone and email (with normalized hashes): {both_count} records")
    print(f"  - Both hashes only (no plain text): {hashed_only_count} records")
    print(f"  - Normalized phone hash only: {normalized_phone_hash_only_count} records")
    print(f"  - Normalized email hash only: {normalized_email_hash_only_count} records")
    
    print(f"\nSample normalization verification:")
    sample_phone = phone_numbers[0]
    sample_email = email_addresses[0]
    normalized_phone = normalize_phone(sample_phone)
    normalized_email = normalize_email(sample_email)
    
    print(f"Phone: {sample_phone} -> Normalized: {normalized_phone} -> Hash: {hashlib.sha256(normalized_phone.encode('utf-8')).hexdigest()}")
    print(f"Email: {sample_email} -> Normalized: {normalized_email} -> Hash: {hashlib.sha256(normalized_email.encode('utf-8')).hexdigest()}")
    
    # Show sample of each record type
    sample_phone_only = next((row for row in rows if row[2] and not row[3] and row[4] and not row[5]), None)
    sample_email_only = next((row for row in rows if row[3] and not row[2] and row[5] and not row[4]), None)
    sample_both = next((row for row in rows if row[2] and row[3] and row[4] and row[5]), None)
    sample_hashed_only = next((row for row in rows if not row[2] and not row[3] and row[4] and row[5]), None)
    sample_norm_phone_only = next((row for row in rows if not row[2] and not row[3] and row[4] and not row[5]), None)
    sample_norm_email_only = next((row for row in rows if not row[2] and not row[3] and not row[4] and row[5]), None)
    
    print(f"\nSample records:")
    if sample_phone_only:
        print(f"Phone only: phone='{sample_phone_only[2]}', normalized_phone_hash='{sample_phone_only[4][:20]}...', email='', email_hash=''")
    if sample_email_only:
        print(f"Email only: email='{sample_email_only[3]}', normalized_email_hash='{sample_email_only[5][:20]}...', phone='', phone_hash=''")
    if sample_both:
        print(f"Both: phone='{sample_both[2]}', email='{sample_both[3]}', both normalized hashes present")
    if sample_hashed_only:
        print(f"Both hashes only: phone='', email='', normalized_phone_hash='{sample_hashed_only[4][:20]}...', normalized_email_hash='{sample_hashed_only[5][:20]}...'")
    if sample_norm_phone_only:
        print(f"Normalized phone hash only: phone='', email='', phone_hash='', normalized_phone_hash='{sample_norm_phone_only[4][:20]}...', email_hash=''")
    if sample_norm_email_only:
        print(f"Normalized email hash only: phone='', email='', phone_hash='', email_hash='', normalized_email_hash='{sample_norm_email_only[5][:20]}...'")
    
    print("\nSchema updates applied:")
    print("- Added phone and email normalization functions")
    print("- Phone normalization: Added +91 prefix")
    print("- Email normalization: Removed special characters before domain")
    print("- All hashes are based on normalized values")
    print("- Added 'normalized_phone_hash_only' record type")
    print("- Added 'normalized_email_hash_only' record type")
    print("- Maintained consistent column count with empty strings for missing data")
    print("- BOOL columns use True/False values")
    print("- DATE column uses YYYY-MM-DD format")
    print("- TIMESTAMP column uses proper UTC format")
    print("- Added birth_year as INT64")
    
    return output_file

if __name__ == "__main__":
    generate_dummy_data()