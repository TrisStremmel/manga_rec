import json
import base64
import mysql.connector
import sys
import numpy as np

ratingMap = {5: 'likes', 4: 'interested', 3: 'neutral', 2: 'dislikes', 1: 'not-interested'}
def convertRating(ratingTuple):
    mangaId = ratingTuple[2]
    status = ratingTuple[3]
    originalRating = ratingTuple[4]

    if originalRating is not None:
        if originalRating >= 7:
            return mangaId, 5
        if originalRating >= 5:
            return mangaId, 3
        else:  # if rating < 5
            return mangaId, 2
    if status == 'reading' or status == 'completed':
        return mangaId, 4
    if status == 'plan_to_read':
        return mangaId, 4
    if status == 'on_hold':
        return mangaId, 3
    if status == 'dropped':
        return mangaId, 2
    # list of assumptions: if you finished it you liked it
    # if you are reading it or plan to read it you are interested in it
    # if you rated it we map that into liked, disliked, neutral
    # on hold maps to neutral (we cannot make any assumptions with on hold imo so neutral is best)
    # dropped maps to disliked since we assume there was something you disliked about it that caused you to drop it

def satisfiesFilters(manga, filters):
    # if I want include check for include exclude overlap
    if not (filters[0][0] <= manga[1] <= filters[0][1]):
        return False  # popularity
    if manga[2] is not None:
        if not (filters[1][0] <= manga[2] <= filters[1][1]):
            return False  # releaseDate
    if manga[3] is not None:
        if not (filters[2][0] <= manga[3] <= filters[2][1]):
            return False  # chapterCount
    for i in range(len(filters[3])):
        if filters[3][i] == "true" and manga[4+i] == 1:  # may need to change true data type
            return False  # exclude status
    for i in range(len(filters[4])):
        if filters[4][i] == "true" and manga[8+i] == 0:  # may need to change true data type
            return False  # include genre
    for i in range(len(filters[5])):
        if filters[5][i] == "true" and manga[26+i] == 0:  # may need to change true data type
            return False  # include theme
    for i in range(len(filters[6])):
        if filters[6][i] == "true" and manga[77+i] == 0:  # may need to change true data type
            return False  # include demographic
    for i in range(len(filters[7])):
        if filters[7][i] == "true" and manga[8+i] == 1:  # may need to change true data type
            return False  # exclude genre
    for i in range(len(filters[8])):
        if filters[8][i] == "true" and manga[26+i] == 1:  # may need to change true data type
            return False  # exclude theme
    for i in range(len(filters[9])):
        if filters[9][i] == "true" and manga[77+i] == 1:  # may need to change true data type
            return False  # exclude demographic
    return True

def getStatusSet(myCursor):
    myCursor.execute("select DISTINCT status from manga")
    return [x[0].replace("\"", "") for x in myCursor]

def getGenreSet(myCursor):
    myCursor.execute("select DISTINCT genre from manga")
    genreClumps = '|'.join([x[0].replace("\"", "") for x in myCursor if x[0] is not None])
    genreSet = set(genreClumps.split("|"))
    return list(genreSet)

def getThemeSet(myCursor):
    myCursor.execute("select DISTINCT theme from manga")
    themeClumps = '|'.join([x[0].replace("\"", "") for x in myCursor if x[0] is not None])
    themeSet = set(themeClumps.split("|"))
    return list(themeSet)

def getDemographicSet(myCursor):
    myCursor.execute("select DISTINCT demographic from manga")
    demographicClumps = '|'.join([x[0].replace("\"", "") for x in myCursor if x[0] is not None])
    demographicSet = set(demographicClumps.split("|"))
    return list(demographicSet)

def encodeManga(myCursor, manga, filters):
    mangaEncoded = []
    statusSet = getStatusSet(myCursor)
    genreSet = getGenreSet(myCursor)
    themeSet = getThemeSet(myCursor)
    demographicSet = getDemographicSet(myCursor)
    for mangaInstance in manga:
        instanceData = [mangaInstance[0], mangaInstance[1], mangaInstance[4], mangaInstance[5]]

        mangaInstanceStatusSet = mangaInstance[6].replace("\"", "")
        for i in range(len(statusSet)):
            if statusSet[i] == mangaInstanceStatusSet:
                instanceData.append(1)
            else:
                instanceData.append(0)
        mangaInstanceGenreSet = mangaInstance[7].replace("\"", "").split('|') if mangaInstance[7] is not None else []
        for i in range(len(genreSet)):
            if genreSet[i] in mangaInstanceGenreSet:
                instanceData.append(1)
            else:
                instanceData.append(0)
        mangaInstanceThemeSet = mangaInstance[8].replace("\"", "").split('|') if mangaInstance[8] is not None else []
        for i in range(len(themeSet)):
            if themeSet[i] in mangaInstanceThemeSet:
                instanceData.append(1)
            else:
                instanceData.append(0)
        mangaInstanceDemographicSet = mangaInstance[9].replace("\"", "").split('|') if mangaInstance[9] is not None else []
        for i in range(len(demographicSet)):
            if demographicSet[i] in mangaInstanceDemographicSet:
                instanceData.append(1)
            else:
                instanceData.append(0)

        if satisfiesFilters(instanceData, filters):
            mangaEncoded.append(instanceData)
    return mangaEncoded

def recommend(userId: int, filters):
    dataBase = mysql.connector.connect(
        host="washington.uww.edu",
        user="stremmeltr18",
        passwd=base64.b64decode(b'dHM1NjEy').decode("utf-8"),
        database="manga_rec"
    )
    myCursor = dataBase.cursor()

    #create one hot encoded (and other data alterations) matrix of movie table
    # possible include no_genres/no_themes column (i dont think it would be good but idk)
    myCursor.execute("select * from manga")
    manga = [x for x in myCursor]
    #0:id, 1:popularity, 2: releaseDate, 3:chapterCount, 4-7:status, 8-25:genre, 26-76:theme, 77-81:demographic
    mangaEncoded = encodeManga(myCursor, manga, filters)
    ##print(manga[0])
    ##print(mangaEncoded[0])

    # get user's manga ratings
    myCursor.execute("select * from ratings where userId = %s", [userId])
    ratings = [x for x in myCursor]
    convertedRatings = [convertRating(x) for x in ratings]
    ##print(convertedRatings)
    # #print('\n'.join([str(x) for x in convertedRatings]))

    #create table of manga the user has rated (make has subtable of one hot table so you dont redo work)
    userTable = []
    filteredRatings = []
    for i in range(len(convertedRatings)):
        for j in range(len(mangaEncoded)):
            if convertedRatings[i][0] == mangaEncoded[j][0]:  # only include manga that have not been filtered out
                userTable.append(mangaEncoded[j][4:])  # only uses the one hot values for now
                filteredRatings.append(list(convertedRatings[i]))
                break
    ##print('\n'.join([str(x) for x in userTable]))
    for i in range(len(manga)):
        manga[i] = list(manga[i])
        del manga[i][3]
        #del manga[i][9]
    #print('\n'.join([str(manga[[i[0] for i in manga].index(filteredRatings[x][0])]) for x in range(len(filteredRatings))]))

    #create a user preference vector (average or dot product of all user ratings)
    userProfile = np.array(userTable).T.dot([i[1] for i in filteredRatings])
    #print([i[1] for i in filteredRatings])
    ##print(', '.join([str(x) for x in userProfile]))
    #print(userProfile)
    ##print(userProfile.shape)

    '''for i in range(len(mangaEncoded)):
        mangaEncoded[i] = mangaEncoded[i][4:]'''
    # create recommendations
    mangaGenreTable = np.delete(np.array(mangaEncoded), np.s_[0:4], axis=1)
    recommendationTable = ((np.array(mangaGenreTable) * userProfile).sum(axis=1))/(userProfile.sum())
    ##print(recommendationTable[0:10])
    ##print(type(recommendationTable))
    recommendations = np.zeros(shape=(recommendationTable.shape[0], 2))
    for i in range(len(recommendationTable)):
        #recommendationTable[i] = np.array([mangaEncoded[i][0], recommendationTable[i]])
        recommendations[i][0] = mangaEncoded[i][0]
        recommendations[i][1] = recommendationTable[i]
    # #print(recommendationTable.shape)
    # #print(recommendations[0:10])
    # #print(recommendationTable[0:10])
    #recommendations = recommendations[recommendations[:, 1].argsort()]
    recommendations = recommendations[recommendations[:, 1].argsort()[::-1]]
    np.set_printoptions(suppress=True)
    #print(recommendations[0:10])
    #print('\n'.join([str(manga[[i[0] for i in manga].index(recommendations[x][0])]) for x in range(len(recommendations[0:10]))]))
    #get the manga rows that were recommended (and exclude those the user has already rated)
    recommendedManga = [manga[[i[0] for i in manga].index(recommendations[x][0])] for x in range(len(recommendations[0:100]))
                        if recommendations[x][0] not in [i[0] for i in userTable]]

    myCursor.close()
    # return list of json with manga info for the highest scored recommendations
    return [json.dumps({"id": x[0], "title": x[2][1:-1], "pictureLink": x[9][1:-1]}) for x in recommendedManga[0:20]]


callFromNode = False
includeAll = [[1, 27691], [1946, 2022], [1, 6477],
              [False] * 4, [False] * 18, [False] * 51, [False] * 5, [False] * 18, [False] * 51, [False] * 5]
if callFromNode:
    userId = int(sys.argv[1])
    filtersIn = sys.argv[2]
    filtersIn = json.loads(filtersIn)

    print(recommend(userId, filtersIn))
    sys.stdout.flush()
else:
    print(recommend(10, includeAll))
