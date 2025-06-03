import csv
import os

def create_navigational_status_csv():
    # Define the data for navigational status codes
    navigational_status_data = [
        [0, "Underway using engine"],
        [1, "At anchor"],
        [2, "Not under command"],
        [3, "Restricted maneuverability"],
        [4, "Constrained by her draught"],
        [5, "Moored"],
        [6, "Aground"],
        [7, "Engaged in fishing"],
        [8, "Underway sailing"],
        [9, "Reserved for future amendment of navigational status for ships carrying DG, HS or IMO hazard or pollutant category C, HSC"],
        [10, "Reserved for future amendment of navigational status for ships carrying DG, HS, MP or IMO hazard or pollutant category A, WIG"],
        [11, "Power-driven vessel towing astern"],
        [12, "Power-driven vessel pushing ahead or towing alongside"],
        [13, "Reserved for future use"],
        [14, "AIS-SART Active, AIS-MOB, AIS-EPIRB"],
        [15, "Undefined"]
    ]
    
    # Get current script directory
    current_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Create file path in the same directory as the script
    csv_path = os.path.join(current_dir, 'navigational_status_code.csv')
    
    # Write data to CSV file
    with open(csv_path, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        # Write header
        writer.writerow(['navigational_status_code', 'navigational_status'])
        # Write data rows
        writer.writerows(navigational_status_data)
    
    print(f"CSV file '{csv_path}' created successfully.")

if __name__ == "__main__":
    create_navigational_status_csv() 