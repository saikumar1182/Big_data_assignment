#Program to merge the 3 datasets and create/write to 4th dataset

#import libraries
import csv
import pandas as pd

#Reads the line and as the data is not clean 
def split_csv_line(line, delimiter=','):
    return list(csv.reader([line], delimiter=delimiter))[0]

#Function cleans the data returns the dataframe 
def read_csv(filepath, delimiter=','):
    with open(filepath, 'r') as file:
        lines = file.readlines()

    headers = split_csv_line(lines[0].strip(), delimiter=delimiter)

    data = []
    for line in lines[1:]:
        fields = split_csv_line(line, delimiter=delimiter)
        
        if len(fields) != len(headers):
            while len(fields) > len(headers):
                fields[-2] = fields[-2] + delimiter + fields[-1]
                del fields[-1]  
                
            if len(fields) != len(headers):
                continue 
        data.append(fields)

    df = pd.DataFrame(data, columns=headers)
    return df

# Converts to lowercase and strip whitespaces
def lowercase_and_strip(df, columns_to_lowercase):
    for col in columns_to_lowercase:
        df[col] = df[col].str.lower().str.strip()
    return df

#Solves the conflicts in data adn deletes the old columns
def resolve_column_conflict(df, website_col, google_col, facebook_col, resolved_col_name):
    def resolver(row):
        if website_col and pd.notna(row[website_col]) and pd.notna(row[google_col]) and row[website_col] != row[google_col]:
            return row[website_col]
        cols_to_check = [col for col in [website_col, google_col, facebook_col] if col]
        return row[cols_to_check].dropna().iloc[0] if not row[cols_to_check].dropna().empty else None

    df[resolved_col_name] = df.apply(resolver, axis=1)
    cols_to_drop = [col for col in [website_col, google_col, facebook_col] if col]
    df.drop(columns=cols_to_drop, inplace=True)
    
    return df


def main():
    #Reading the datasets to dataframes
    website_df = read_csv('/home/data/website_dataset.csv',delimiter=';')
    google_df = read_csv('/home/data/google_dataset.csv',delimiter=',')
    facebook_df = read_csv('/home/data/facebook_dataset.csv',delimiter=',')

    #Renaming the dataframe column names
    website_df.columns= ['domain', 'domain_suffix', 'language', 'legal_name', 'city','country', 'region_name', 'phone', 'name', 'tld','category']
    google_df.columns= ['address', 'category', 'city', 'country_code', 'country', 'name','phone', 'phone_country_code', 'raw_address', 'raw_phone','region_code', 'region_name', 'text', 'zip_code', 'domain']
    facebook_df.columns= ['domain', 'address', 'category', 'city', 'country_code','country', 'description', 'email', 'link', 'name', 'page_type','phone', 'phone_country_code', 'region_code', 'region_name','zip_code']
    
    # Dropping the unwanted columns
    website_df = website_df.drop(['domain_suffix','language','tld'],axis=1)
    google_df = google_df.drop(['text','phone_country_code','region_code','country_code','raw_address','raw_phone', 'zip_code'],axis=1)
    facebook_df = facebook_df.drop(['description','phone_country_code','region_code','zip_code', 'country_code'],axis=1)


    # Converting specfic columns to lowercase for comparision
    website_col_convert = ['domain', 'city', 'country','region_name','name','category']
    website_lower_df = lowercase_and_strip(website_df, website_col_convert)

    google_col_convert = ['domain', 'city','address','country','region_name','name','category']
    google_lower_df = lowercase_and_strip(google_df, google_col_convert)

    facebook_col_convert = ['domain', 'city','address','country','region_name','name','category']
    facebook_lower_df = lowercase_and_strip(facebook_df, facebook_col_convert)


    # Merging the dataframes based on domain column
    merged_df = website_df.merge(google_df, on='domain', how='left', suffixes=('_website','_google'))
    merged_df = merged_df.merge(facebook_df, on='domain', how='left',suffixes= ('','_facebook'))


    # Solving the conflict on the specific require columns
    merged_df = resolve_column_conflict(merged_df, 'name_website', 'name_google', 'name', 'resolved_name')
    merged_df = resolve_column_conflict(merged_df, '', 'address', 'address_facebook', 'resolved_address')
    merged_df = resolve_column_conflict(merged_df, 'category_website', 'category_google', 'category', 'resolved_category')
    merged_df = resolve_column_conflict(merged_df, 'phone_website', 'phone_google', 'phone', 'resolved_phone')
    merged_df = resolve_column_conflict(merged_df, 'city_website', 'city_google', 'city', 'resolved_city')
    merged_df = resolve_column_conflict(merged_df, 'country_website', 'country_google', 'country', 'resolved_country')
    merged_df = resolve_column_conflict(merged_df, 'region_name_website', 'region_name_google', 'region_name', 'resolved_region_name')

    #Arranging the columns in an order to more simplicity
    ordered_cols = ['domain', 'resolved_name', 'resolved_category','resolved_address', 'resolved_city', 'resolved_country',
                'resolved_region_name', 'resolved_phone','legal_name', 'email', 'link', 'page_type']
    merged_df=merged_df[ordered_cols]

    # Write the merged dataset to directory.
    merged_df.to_csv('/home/data/merged_dataset.csv')
    

if __name__ == '__main__':
    main()