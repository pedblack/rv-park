import re

import pandas as pd

# 1. Load the CSV file
filename = "data/backbone_locations_v2.csv"
df = pd.read_csv(filename)


# 2. Function to parse counts from the tag strings
def parse_counts(row_data, target_tag_name):
    total_occurrences = 0
    target_occurrences = 0

    if pd.isna(row_data):
        return 0, 0

    # Split the string by semicolon to get individual tags
    tags = str(row_data).split(";")

    for tag in tags:
        tag = tag.strip()
        if not tag:
            continue

        # Regex to capture the tag name and the number in parentheses
        # Example: "atmosphere_quiet_peaceful (7)" -> name="atmosphere_quiet_peaceful", count="7"
        match = re.match(r"(.+?)\s*\((\d+)\)$", tag)

        if match:
            name = match.group(1).strip()
            count = int(match.group(2))

            # Sum total occurrences (the number in parenthesis)
            total_occurrences += count

            # Check if this tag matches the target "misc" tag
            if name == target_tag_name:
                target_occurrences += count

    return total_occurrences, target_occurrences


# 3. Process 'ai_pros' for 'misc_other_pros'
total_pros_count = 0
misc_pros_count = 0

for value in df["ai_pros"]:
    t_count, m_count = parse_counts(value, "misc_other_pros")
    total_pros_count += t_count
    misc_pros_count += m_count

# 4. Process 'ai_cons' for 'misc_other_cons'
total_cons_count = 0
misc_cons_count = 0

for value in df["ai_cons"]:
    t_count, m_count = parse_counts(value, "misc_other_cons")
    total_cons_count += t_count
    misc_cons_count += m_count

# 5. Calculate and print rates
pros_rate = (misc_pros_count / total_pros_count) if total_pros_count > 0 else 0
cons_rate = (misc_cons_count / total_cons_count) if total_cons_count > 0 else 0

print(f"Total Pros Occurrences: {total_pros_count}")
print(f"Misc Pros Occurrences: {misc_pros_count}")
print(f"Pros Misc Rate: {pros_rate:.2%}")
print("-" * 30)
print(f"Total Cons Occurrences: {total_cons_count}")
print(f"Misc Cons Occurrences: {misc_cons_count}")
print(f"Cons Misc Rate: {cons_rate:.2%}")
