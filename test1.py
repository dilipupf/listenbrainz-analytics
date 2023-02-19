import csv
import sys
import os
import json
import pprint
import time

#add current working directory to sys path
current_dir = os.getcwd()
sys.path.append(current_dir)
# Get the parent directory path

#print(sys.path)
print('************* Executing test.py *************')

# listens data file path using os join command
LISTENS_DATA_PATH =  os.path.join(current_dir, 'listens_data')
print('listens data path: ', LISTENS_DATA_PATH)

#input data files
USER_LISTENING_DATA_PATH = 't2_user_listening_data.csv'
LISTEN_BRAINZ_PATH = 'listenbrainz_msid_mapping.csv'
CAN_MUSIC_BRAINZ_PATH = 'canonical_musicbrainz_data-002.csv'
ARTIST_MBID_NAME_PATH = 'musicbrainz_artist_mbid_name.csv'

# output data file names for csv file creation and processing
COUNT_OF_USERLISTENINGS_FILENAME = 'o1_count_of_user_listenings.csv'
USERDATA_LISTENBRAINZ_MERGE_FILENAME = 'o1_user_listenbrainz_merge.csv'
USER_LISTENBRAINZ_MUSICBRAINZ_MERGE_FILENAME = 'o1_user_listenbrainz_musicbrainz_merge.csv'
USER_LISTENBRAINZ_MUSICBRAINZ_ARTISTMBID_MERGE_FILENAME = 'o1_user_listenbrainz_musicbrainz_artistmbid_merge.csv'

# join column names
USERDATA_AND_LISBRAINZ_COMMON_COLUMN = 'recording_msid'
USERDATA_LISBRAINZ_AND_CANMUSICBRAINZ_COMMON_COLUMN = 'recording_mbid'
USERLISTENMUSICBRAINZ_ARTISTMBID_COMMON_COLUMN = 'artist_mbids'

def extract_json_lines(folder_path, output_file_name):
    
    parent_dir = os.path.dirname(folder_path)
    csv_path = os.path.join(parent_dir, output_file_name)

    with open(csv_path, 'w', newline='') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(['user_id', 'recording_msid'])

        for filename in os.listdir(folder_path):
            if filename.endswith('.listens'):
                file_path = os.path.join(folder_path, filename)

                with open(file_path, 'r') as json_file:
                    for line in json_file:
                        data = json.loads(line)
                        user_id = data['user_id']
                        recording_msid = data['recording_msid']
                        writer.writerow([user_id, recording_msid])
                        #save the file after every write
                        csv_file.flush()

        return csv_file
                        

def find_column_numbers(csv_file, header_names):
    with open(csv_file, 'r') as f:
        reader = csv.reader(f)
        headers = next(reader) # read the headers

        # create a dictionary to store the header names and column numbers
        header_map = {header: i for i, header in enumerate(headers)}

        # find the column numbers for the given header names
        column_numbers = []
        for header_name in header_names:
            column_number = header_map.get(header_name)
            if column_number is not None:
                column_numbers.append(column_number)

        return column_numbers

def slice_csvfile(csv_file, column_numbers, out_file):
    with open(csv_file, 'r') as f, open(out_file, 'w', newline='') as out_f:
        reader = csv.reader(f)
        headers = next(reader) # read the headers

        # write the headers to the new file
        out_writer = csv.writer(out_f)
        out_writer.writerow([headers[i] for i in column_numbers])

        # write the data to the new file
        for row in reader:
            out_writer.writerow([row[i] for i in column_numbers])
            out_f.flush() # Save the file after every write
        
        return out_file

#count the number of listenings for each artist_name and add new column to the csv file called count for each row
def count_number_of_listenings(csv_file, out_file):
    with open(csv_file, 'r') as f:
        reader = csv.reader(f)
        headers = next(reader) # read the headers

        # create a new csv file to store the data remove the columns recording_msid, recording_mbid
        # and add a new column called count
        
        with open(out_file, 'w', newline='') as out_f:
            out_writer = csv.writer(out_f)
            out_writer.writerow(['user_id', 'artist_mbid','artist_name', 'count'])

            # create a dictionary to store the artist_name and the number of listenings
            listening_details = {}

            # create a for loop to iterate through the csv file and count the number of listenings for each artist_name and also add
            # recording_mbid and user_id to the dictionary
            for row in reader:
                user_id = row[0]
                artist_mbid = row[3]
                artist_name = row[4]

                # check if the artist_name is in the dictionary
                if artist_name in listening_details:
                    listening_details[artist_name]['count'] += 1
                else:
                    listening_details[artist_name] = {'count': 1, 'artist_mbid': artist_mbid, 'user_id': user_id}
            
            # write the data to the new csv file
            for artist_name, details in listening_details.items():
                out_writer.writerow([details['user_id'], details['artist_mbid'], artist_name, details['count']])
                out_f.flush()

class csvPreProcess:
    def __init__(self, file1_path, file2_path, file1_primary_key_col, file2_primary_key_col, file2_columns):
        self.file1_path = file1_path
        self.file2_path = file2_path
        self.file1_primary_key_col = file1_primary_key_col
        self.file2_primary_key_col = file2_primary_key_col
        self.file2_columns = file2_columns



    def merge_csv(self, output_file):
        # try:
            file1_dict = {}
            with open(self.file1_path, 'r') as file1:
                file1_reader = csv.reader(file1)
                file1_header = next(file1_reader) 
                # print('file1_header', file1_header)
                # print('file1_header[self.file2_columns]',  self.file2_columns)
               
                for row in file1_reader:
                    file1_dict[row[self.file1_primary_key_col]] = row
                    

            with open(self.file2_path, 'r') as file2, open(output_file, 'w') as output:
                file2_reader = csv.reader(file2)
                file2_header = next(file2_reader)
               
                output_writer = csv.writer(output)

                # Write header row to output file
                output_writer.writerow(file1_header + [file2_header[col] for col in self.file2_columns])
                # print('file2_header', [file2_header[col] for col in self.file2_columns])
            
                for row in file2_reader:
                    #print('row', type(row[2]))
                    if row[self.file2_primary_key_col] in file1_dict:
                        #try:

                           
                            # if (file2_header[i] == 'artist_mbids' for i in self.file2_columns):
                                
                            #     artist_mbids = [row[i] for i in self.file2_columns]
                            #     # print('artist_mbids', artist_mbids)
                            #     # for artist_mbid in artist_mbids:
                            #     output_writer.writerow(file1_dict[row[self.file1_primary_key_col]] +  artist_mbids[0] ) 
                            #     # save the output after every write
                            #     output.flush() 
                            # else:
                                # If primary key exists in file1, append columns from file2 to row in file1
                                output_writer.writerow(file1_dict[row[self.file2_primary_key_col]] + [row[col].strip("{}").split(",")[0] for col in self.file2_columns])
                                #print('[row[col] for col in self.file2_columns]', [row[col].strip("{}").split(",")[0] for col in self.file2_columns][0])
                                # save the output after every write
                                output.flush()

                        # except Exception as e:
                        #     print('Error while writing info from csv file :'+self.file2_path, e)
            
                
        

        # except Exception as e:
        #     # add line number to the error message and type of error
        #     print('Error in merging csv files at line number: ', sys.exc_info()[-1].tb_lineno)
        #     print('Error type: ', sys.exc_info()[0])


def return_primkey_col_nums(file_path1, file_path2, primkey):
    try:
        file1_cols = find_column_numbers(file_path1, [primkey])[0]
        file2_cols = find_column_numbers(file_path2, [primkey])[0]
        return file1_cols, file2_cols
    except Exception as e:
        print('Error while finding primary key column numbers', e)

if __name__ == '__main__':
    print(' Preprocessing started ... ')
    start = time.monotonic()
    # print('Processing user listening data ... ')
    # extract_json_lines(LISTENS_DATA_PATH, USER_LISTENING_DATA_PATH)
    # print('User listening data files processed successfully ... ')
    end = time.monotonic()
    print('Time taken to process user listening data: ', end - start)


    
    print('Merging user lisetning data and listen brainz data based on messybrainz id as common key ... ')
    user_data_pk_col, lisbrainz_data_pk_col = return_primkey_col_nums(USER_LISTENING_DATA_PATH, LISTEN_BRAINZ_PATH , USERDATA_AND_LISBRAINZ_COMMON_COLUMN)
    print('user_data_pk_col', user_data_pk_col)
    print('lismusbrainz_data_pk_col', lisbrainz_data_pk_col)

    print('Finding column numbers for recording_mbid in listen brainz data ... ')
    listen_brainz_col_nums = find_column_numbers(LISTEN_BRAINZ_PATH, ['recording_mbid'])
    print('listen_brainz_col_nums', listen_brainz_col_nums)
    mergeUserListeningData = csvPreProcess(USER_LISTENING_DATA_PATH, LISTEN_BRAINZ_PATH, user_data_pk_col, lisbrainz_data_pk_col, listen_brainz_col_nums )
    print('Adding recording mbid number...')
    mergeUserListeningData.merge_csv(USERDATA_LISTENBRAINZ_MERGE_FILENAME)
    print('Merging user listening data and listen brainz data completed successfully!')
    end2 = time.monotonic()
    print('Time taken to merge user listening data and listen brainz data(end2 - end): ', end2 - end)

    

    # print('Merging user lisetning data, listen brainz data with canonical music brainz data based on recording_mbid as common key ... ')
    # userlistenbrainz_data_pk_col, can_musicbrainz_data_pk_col = return_primkey_col_nums(USERDATA_LISTENBRAINZ_MERGE_FILENAME, CAN_MUSIC_BRAINZ_PATH , USERDATA_LISBRAINZ_AND_CANMUSICBRAINZ_COMMON_COLUMN)
    # print('userlistenbrainz_data_pk_col', userlistenbrainz_data_pk_col)
    # print('can_musicbrainz_data_pk_col', can_musicbrainz_data_pk_col)

    # print('Searching for artists_mbid based on recording_mbid  ...')
    # can_musicbrainz_col_nums = find_column_numbers(CAN_MUSIC_BRAINZ_PATH, ['artist_mbids'])
    # print('can_musicbrainz_col_nums', can_musicbrainz_col_nums)
    # mergeUserListeningData = csvPreProcess(USERDATA_LISTENBRAINZ_MERGE_FILENAME, CAN_MUSIC_BRAINZ_PATH, userlistenbrainz_data_pk_col, can_musicbrainz_data_pk_col, can_musicbrainz_col_nums )
    # mergeUserListeningData.merge_csv(ARTIST_MBID_NAME_PATH)
    # print(' Merging csv files and added artist_mbid column succesfully! ')

    # end3 = time.monotonic()
    # print('Time taken to merge user listening data, listen brainz data with canonical music brainz data: ', end3 - end2)



   

    # print('Looking for arist name based on artist_mbid ... ')
    # artist_data_pk_col = find_column_numbers(ARTIST_MBID_NAME_PATH, ['mbid'])[0]
    # user_listenbrainz_musicbrainz_merge_file_pk_col = find_column_numbers(USER_LISTENBRAINZ_MUSICBRAINZ_MERGE_FILENAME, [USERLISTENMUSICBRAINZ_ARTISTMBID_COMMON_COLUMN])[0]
    # print('user_listenbrainz_musicbrainz_merge_file_pk_col', user_listenbrainz_musicbrainz_merge_file_pk_col)
    # print('artist_data_pk_col', artist_data_pk_col)

    # print('Searching for artist name based on artist_mbids  ...')
    # artist_mbid_name_cols = find_column_numbers(ARTIST_MBID_NAME_PATH, ['name'])
    # print('artist_mbid_name_cols', artist_mbid_name_cols)
    # mergeUserListeningData = csvPreProcess(USER_LISTENBRAINZ_MUSICBRAINZ_MERGE_FILENAME, ARTIST_MBID_NAME_PATH, user_listenbrainz_musicbrainz_merge_file_pk_col, artist_data_pk_col, artist_mbid_name_cols )
    # mergeUserListeningData.merge_csv()
    # print(' Merging csv files and added artist_mbid column succesfully! ')

    # print('Counting number of listenings for each artist ... ')
    # count_number_of_listenings(COUNT_OF_USERLISTENINGS_FILENAME)
    # print('Counting number of listenings for each artist completed successfully! ')
    # end4 = time.monotonic()
    # print('Time taken to merge user listening data, listen brainz data with canonical music brainz data: ', end4 - end3)

    # print(' Preprocessing completed successfully!')

   
