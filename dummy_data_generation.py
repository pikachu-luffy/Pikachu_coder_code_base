import hashlib
import csv
import random
from datetime import datetime, timedelta

def generate_dummy_data():
    """Generate dummy data CSV with phone numbers and their SHA256 hashes"""
    
    # Phone numbers provided
    phone_numbers = [
        "9873185132", "9997297420", "8090780377", "7017890375", "7409898048",
        "7906063900", "7906310216", "9927173568", "8046396150", "8851872499",
        "7619977823", "9958446833", "8460779976", "8035059316", "9458744602",
        "8390690180", "8047943528"
    ]
    
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
    flags = ["Yes", "No"]
    preferences = ["Sweet", "Bitter", "Milk Chocolate", "Dark Chocolate", "White Chocolate"]
    
    # CSV header based on schema
    header = [
        "first_name", "last_name", "phone", "phone_number_hashed", "email_hashed", "gaid", "idfa",
        "birthday", "city", "gender", "state", "source", "consumption_brand_preference",
        "brand_consumption_frequency", "parent_flag", "liked_recipes_dessert_preference",
        "scan_code_flag", "pincode", "packscan_packtype", "chocolate_consumption", "romance",
        "sports", "cadbury_brand_dessert_preference", "mithai_consumption_flag",
        "candies_consumption_flag", "biscuit_consumption_flag", "age", "coupon_flag"
    ]
    
    # Generate data rows
    rows = []
    
    for phone in phone_numbers:
        # Hash the phone number using SHA256
        phone_hashed = hashlib.sha256(phone.encode('utf-8')).hexdigest()
        
        # Generate dummy email and hash it
        first_name = random.choice(first_names)
        last_name = random.choice(last_names)
        email = f"{first_name.lower()}.{last_name.lower()}@example.com"
        email_hashed = hashlib.sha256(email.encode('utf-8')).hexdigest()
        
        # Generate random birthday (age 18-65)
        age = random.randint(18, 65)
        birth_year = datetime.now().year - age
        birthday = f"{random.randint(1, 28):02d}/{random.randint(1, 12):02d}/{birth_year}"
        
        # Generate random pincode (6 digits)
        pincode = f"{random.randint(100000, 999999)}"
        
        # Generate random GAID and IDFA (UUID-like format)
        gaid = f"{random.randint(10000000, 99999999)}-{random.randint(1000, 9999)}-{random.randint(1000, 9999)}-{random.randint(1000, 9999)}-{random.randint(100000000000, 999999999999)}"
        idfa = f"{random.randint(10000000, 99999999)}-{random.randint(1000, 9999)}-{random.randint(1000, 9999)}-{random.randint(1000, 9999)}-{random.randint(100000000000, 999999999999)}"
        
        row = [
            first_name,
            last_name,
            phone,
            phone_hashed,
            email_hashed,
            gaid,
            idfa,
            birthday,
            random.choice(cities),
            random.choice(genders),
            random.choice(states),
            random.choice(sources),
            random.choice(brands),
            random.choice(frequencies),
            random.choice(flags),
            random.choice(preferences),
            random.choice(flags),
            pincode,
            random.choice(["Pouch", "Box", "Wrapper", "Bottle", "Can"]),
            random.choice(frequencies),
            random.choice(flags),
            random.choice(flags),
            random.choice(preferences),
            random.choice(flags),
            random.choice(flags),
            random.choice(flags),
            str(age),
            random.choice(flags)
        ]
        
        rows.append(row)
    
    # Write to CSV file
    output_file = "dummy_phone_data.csv"
    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(header)
        writer.writerows(rows)
    
    print(f"Generated {len(rows)} rows of dummy data in '{output_file}'")
    print(f"Sample phone number hash: {phone_numbers[0]} -> {hashlib.sha256(phone_numbers[0].encode('utf-8')).hexdigest()}")
    
    return output_file

if __name__ == "__main__":
    generate_dummy_data()