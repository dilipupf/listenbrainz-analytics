import csv
import sys
import os
import json
import time

#add current working directory to sys path
current_dir = os.getcwd()
sys.path.append(current_dir)
# Get the parent directory path

#print(sys.path)
print('************* Executing test3.py *************')

# listens data file path using os join command
LISTENS_DATA_PATH =  os.path.join(current_dir, 'listens_data')

#input data files
USER_LISTENING_DATA_PATH = 't1_user_listening_data.csv'
LISTEN_BRAINZ_PATH = 'listenbrainz_msid_mapping.csv'
CAN_MUSIC_BRAINZ_PATH = 'canonical_musicbrainz_data-002.csv'
ARTIST_MBID_NAME_PATH = 'musicbrainz_artist_mbid_name.csv'

# output data file names for csv file creation and processing
FINAL_WITHOUT_PLAYCOUNT = 'final4.csv'
FINAL_WITH_PLAYCOUNT = 'final4-playcount.csv'



def csv_to_dict(file_path, key_column, value_column):
    start_time = time.monotonic()

    with open(file_path, 'r') as csv_file:
        reader = csv.reader(csv_file)
        header = next(reader)  # Get the header row
        key_index = header.index(key_column)
        value_index = header.index(value_column)
        result_dict = {}
        for row in reader:
            key = row[key_index]
            if value_column == 'artist_mbids':
                value = row[value_index].strip("{}").split(",")[0]
            else:
                value = row[value_index]

            result_dict[key] = value

    end_time = time.monotonic()
    print(len(result_dict.keys()))
    print(f"Time taken for {file_path}: {end_time - start_time:.2f} seconds")
    return result_dict


def create_file_in_path(file_name, recmbid_to_artistmbid):
  with open(file_name, 'w') as f:
    json.dump(recmbid_to_artistmbid, f)
  
def print_dict(d):
        print("First 5 dictionary items:")
        for i, (key, value) in enumerate(d.items()):
            if i >= 5:
                break
            print(f"{key}: {value}")

def add_play_count_column(filename, final_with_playcount):
    # Open the CSV file and read the rows using csv.reader
    with open(filename, 'r') as f:
        reader = csv.reader(f)
        rows = list(reader)


     # Create a set to store unique rows
    unique_rows = set()

    # Create a dictionary to store the play count for each artist and user
    play_count = {}
    row_count = 0

    # Iterate over the rows and count the plays for each artist and user
    for row in rows[1:]:  # skip the header row
        user_id, artist_mbids, artist_name = row
        key = (user_id, artist_name)
        play_count[key] = play_count.get(key, 0) + 1
        unique_rows.add((user_id, artist_mbids, artist_name)) # Add unique row to set

    if(row_count % 500000 == 0):
        print(f"Play Count added for {row_count} rows")

    # Modify the rows to add the play count column
    header = rows[0]
    header.append('play_count')
    modified_rows = [header] # start with the header row

    for row in unique_rows:
        user_id, artist_mbids, artist_name = row
        key = (user_id, artist_name)
        count = play_count[key]
        modified_rows.append([user_id, artist_mbids, artist_name, count])

    # Write the modified rows back to the CSV file
    with open(final_with_playcount, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(modified_rows)




if __name__ == '__main__':

    start_time = time.monotonic()
    #usrid_to_msid = csv_to_dict(USER_LISTENING_DATA_PATH, 'user_id', 'recording_msid')
    #print_dict(usrid_to_msid)
    # create_file_in_path('usrid_to_msid.json', usrid_to_msid)

    msid_to_mbid = csv_to_dict(LISTEN_BRAINZ_PATH, 'recording_msid', 'recording_mbid') 
    print_dict(msid_to_mbid)
    # create_file_in_path('msid_to_mbid.json', msid_to_mbid)

    recmbid_to_artistmbid = csv_to_dict(CAN_MUSIC_BRAINZ_PATH, 'recording_mbid', 'artist_mbids')
    print_dict(recmbid_to_artistmbid)
    # create_file_in_path('recmbid_to_artistmbid.json', recmbid_to_artistmbid)

    artmbid_to_artname = csv_to_dict(ARTIST_MBID_NAME_PATH, 'mbid', 'name')
    print_dict(artmbid_to_artname)
    # create_file_in_path('artmbid_to_artname.json', artmbid_to_artname)
    print('************* Finished test2.py *************')

    # load the csv file t1_user_listening_data.csv
    with open(USER_LISTENING_DATA_PATH, 'r') as csv_file,  open(FINAL_WITHOUT_PLAYCOUNT, 'w') as f:
        csv_reader = csv.reader(csv_file)
        # skip the header row
        header = next(csv_reader)
        writer = csv.writer(f)
        # write the header row with user_id, recording_msid, recording_mbid, artist_mbids, artist_name
        writer.writerow(['user_id', 'artist_mbids', 'artist_name'])

        recording_msid_not_found = 0
        recording_mbid_not_found = 0
        artist_mbids_not_found = 0

        row_count = 0
        # loop through reach csv_reader row and create a csv file and save the file on each iteration
        for row in csv_reader:


            # Print a message every 50,000 rows
            row_count += 1
            if row_count % 500000 == 0:
                print(f"Processed {row_count} rows")
                
            # unpack the row
            user_id, recording_msid = row
            # print(user_id, recording_msid)
            # lookup the recording_msid in the msid_to_mbid dictionary

            # if the recording_msid is found in the dictionary, then get the recording_mbid
            if recording_msid in msid_to_mbid:
                recording_mbid = msid_to_mbid[recording_msid]
                # print(recording_mbid)
                
                # if the recording_mbid is found in the dictionary, then get the artist_mbids
                if recording_mbid in recmbid_to_artistmbid:
                    artist_mbids = recmbid_to_artistmbid[recording_mbid]
                    # print(artist_mbids)

                     # if the artist_mbids is found in the dictionary, then get the artist_name
                    if artist_mbids in artmbid_to_artname:
                        artist_name = artmbid_to_artname[artist_mbids]
                        # print(artist_name)
                        # write the user_id, recording_msid, recording_mbid, artist_mbids, artist_name to the csv file
                        
                        writer.writerow([user_id,artist_mbids, artist_name])
                        # save the csv file for each iteration
                        f.flush()

                    else:
                        artist_mbids_not_found += 1
                else:
                    recording_mbid_not_found += 1
            else:
                recording_msid_not_found += 1

    print(f"recording_msid_not_found: {recording_msid_not_found}")
    print(f"recording_mbid_not_found: {recording_mbid_not_found}")
    print(f"artist_mbids_not_found: {artist_mbids_not_found}")

    end_time = time.monotonic()
    print(f"Time taken : {end_time - start_time:.2f} seconds")
    print('csv file created')

    # add the play count column to the csv file and saves as final3-playcount.csv
    add_play_count_column(FINAL_WITHOUT_PLAYCOUNT, FINAL_WITH_PLAYCOUNT)
    print('csv file created with play count column')

            

