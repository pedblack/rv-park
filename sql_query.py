import pandas as pd
import sqlite3
import re

# 1. Load the CSV
csv_file = 'backbone_locations.csv'
df = pd.read_csv(csv_file)

# 2. Function to clean and split the pros/cons strings
def process_tags(row, column_name, tag_type):
    if pd.isna(row[column_name]):
        return []
    
    # Split by semicolon
    raw_tags = str(row[column_name]).split(';')
    
    cleaned_tags = []
    for tag in raw_tags:
        tag = tag.strip()
        if not tag:
            continue
            
        # Regex to remove the frequency count at the end, e.g., "Quiet (5)" -> "Quiet"
        clean_tag = re.sub(r'\s*\(\d+\)$', '', tag)
        
        if clean_tag:
            cleaned_tags.append({
                'p4n_id': row['p4n_id'],
                'tag_type': tag_type, # 'pro' or 'con'
                'tag': clean_tag
            })
    return cleaned_tags

# 3. Process the data
all_tags = []
for index, row in df.iterrows():
    # Process pros and cons based on the column names in your CSV
    all_tags.extend(process_tags(row, 'ai_pros', 'pro'))
    all_tags.extend(process_tags(row, 'ai_cons', 'con'))

# Create a DataFrame for the tags
tags_df = pd.DataFrame(all_tags)

# 4. Create DB connection
conn = sqlite3.connect(':memory:')

# 5. Write the tags to their own SQL table
tags_df.to_sql('location_tags', conn, index=False, if_exists='replace')

# ---------------------------------------------------------
# EXECUTE SQL AND WRITE TO FILE
# ---------------------------------------------------------

# Define the output filename
output_file = 'distinct_tags.txt'

with open(output_file, 'w', encoding='utf-8') as f:
    
    # --- PROS ---
    f.write("=== DISTINCT PROS ===\n")
    pro_query = """
    SELECT DISTINCT tag 
    FROM location_tags 
    WHERE tag_type = 'pro' 
    ORDER BY tag ASC;
    """
    pros = pd.read_sql_query(pro_query, conn)
    
    # Iterate through the results and write to file
    for index, row in pros.iterrows():
        f.write(f"- {row['tag']}\n")

    f.write("\n" + "="*30 + "\n\n")

    # --- CONS ---
    f.write("=== DISTINCT CONS ===\n")
    con_query = """
    SELECT DISTINCT tag 
    FROM location_tags 
    WHERE tag_type = 'con' 
    ORDER BY tag ASC;
    """
    cons = pd.read_sql_query(con_query, conn)
    
    for index, row in cons.iterrows():
        f.write(f"- {row['tag']}\n")

print(f"Successfully wrote distinct pros and cons to {output_file}")

conn.close()