import fcntl
import time
import os
from sys import getsizeof
import json
import fileinput

# ----------------------------------------------------------------------------
# CLASS : KeyValDataStoreException
# ---------------------------------------------------------------------------
# REPRESENTS	: User Defined Exception Handling 
# ---------------------------------------------------------------------------
# DATA MEMBERS	: ErrNo, ErrMsg
# ---------------------------------------------------------------------------
# MEMBER FUNCTIONS	: __init__(self, ErrNo, ErrMsg), __str__(self)
# ---------------------------------------------------------------------------
class KeyValDataStoreException(Exception):
    ErrNo = None
    ErrMsg = None
    def __init__(self, ErrNo, ErrMsg):
        self.ErrNo = ErrNo
        self.ErrMsg = ErrMsg
    def __str__(self):
        return "Error encountered.\nError No: " + self.ErrNo + "\nError Message: " + self.ErrMsg

# ----------------------------------------------------------------------------
# CLASS : KeyValDataStore
# ---------------------------------------------------------------------------
# REPRESENTS	: Class to perform CRD operations on data
# ---------------------------------------------------------------------------
# DATA MEMBERS	: filepath
# ---------------------------------------------------------------------------
# MEMBER FUNCTIONS	: __init__(self, filepath), __hash_function(self, key),
#                   __add_object(self, key, value), __remove_object(self, key)
#                   create(self, key, value, ttl), read(self, key), delete(self, key)
# ---------------------------------------------------------------------------
class KeyValDataStore:
    filepath = None
    def __init__(self, filepath = None):
        if filepath is None:
            # initialise with default file path
            self.filepath = '/mnt/c/Users/joshi/Documents/RJ/Python/db.txt'
            pass
        else:
            # initialise with given file path
            self.filepath = filepath
            pass

    # ---------------------------------------------------------------------------
    # PRIVATE UTILITY FUNCTIONS SECTION
    # ---------------------------------------------------------------------------

    # ---------------------------------------------------------------------------
    # FUNCTION NAME : __hash_function()
    # ---------------------------------------------------------------------------
    # PARAMETERS    : key
    # ---------------------------------------------------------------------------
    # RETURN        : hash_value
    # ---------------------------------------------------------------------------
    # DESCRIPTION   : Calculates hash value of a string which maps to line number
    #               in the database file to increase read and search time efficiency.
    #               Collision probablity is approximately 1/m according to the
    #               referenced document. Since m is large enough, collision can
    #               can be ignored.
    # ---------------------------------------------------------------------------
    def __hash_function(self, key):
        # Ref: https://cp-algorithms.com/string/string-hashing.html
        p, m, p_pow = 31, 1000000009, 1
        hash_value = 0
        for char in key:
            hash_value = (hash_value + (ord(char) - ord('A') + 1) * p_pow) % m
            p_pow = (p_pow * p) % m
        return hash_value

    # ---------------------------------------------------------------------------
    # FUNCTION NAME : __add_object()
    # ---------------------------------------------------------------------------
    # PARAMETERS    : key, value
    # ---------------------------------------------------------------------------
    # RETURN        : None
    # ---------------------------------------------------------------------------
    # DESCRIPTION   : Converts to JSON format and adds to corresponding line in 
    #               database file based on hash value returned by the hash function
    #               if it is not a duplicate key.
    #               Raises exception if file is locked by other threads.
    # ---------------------------------------------------------------------------
    def __add_object(self, key, value):
        try:
            fcntl.flock(self.filepath, fcntl.LOCK_EX | fcntl.LOCK_NB)
            hash_value = self.__hash_function(key)
            for line in fileinput.input(self.filepath,inplace=True):
                line = line.strip()
                if fileinput.lineno() != hash_value:
                    print(line)
                else:
                    if line.split()[0] == key:
                        raise KeyValDataStoreException("008", "Cannot insert duplicate key.")
                    print(json.dumps(key),json.dumps(value))
            fcntl.flock(self.filepath, fcntl.LOCK_UN)
            print("Key added")
        except:
            raise KeyValDataStoreException("004", "Cannot be added, file currently in use.")
        
    # ---------------------------------------------------------------------------
    # FUNCTION NAME : __remove_object()
    # ---------------------------------------------------------------------------
    # PARAMETERS    : key
    # ---------------------------------------------------------------------------
    # RETURN        : None
    # ---------------------------------------------------------------------------
    # DESCRIPTION   : Erases particular line from database file based on hash value
    #               returned by the hash function only if that key exists. Raises
    #               exception if file is locked by other threads.
    # ---------------------------------------------------------------------------
    def __remove_object(self, key):
        try:
            fcntl.flock(self.filepath, fcntl.LOCK_EX | fcntl.LOCK_NB)
            hash_value = self.__hash_function(key)
            for line in fileinput.input(self.filepath,inplace=True):
                line = line.strip()
                if fileinput.lineno() != hash_value:
                    print(line)
                else:
                    if line.startswith(json.dumps(key)):
                        print('')
                    else:
                        raise KeyValDataStoreException("006", "Cannot be deleted, entered key does not exist.")
            fcntl.flock(self.filepath, fcntl.LOCK_UN)
            print("Key removed")
        except:
            raise KeyValDataStoreException("005", "Cannot be deleted, file currently in use.")

    
    # ---------------------------------------------------------------------------
    # PUBLIC FUNCTIONS SECTION
    # ---------------------------------------------------------------------------

    # ---------------------------------------------------------------------------
    # FUNCTION NAME : create()
    # ---------------------------------------------------------------------------
    # PARAMETERS    : key, value, ttl
    # ---------------------------------------------------------------------------
    # RETURN        : None
    # ---------------------------------------------------------------------------
    # DESCRIPTION   : Function to create a new record, with optional TTL. Assumes that the
    #               parameters are JSON objects and not Python objects. Handles exceptions
    #               and specified business conditions.
    # ---------------------------------------------------------------------------
    def create(self, key, value, ttl = None):
        key = json.loads(key)
        if len(key) > 32:
            raise KeyValDataStoreException("001", "Invalid Key. Entered key should not exceed 32 characters.")
        value = json.loads(value)
        if type(value) != dict:
            raise KeyValDataStoreException("002", "Invalid Value. Entered value is not a valid JSON Object.")
        if getsizeof(value) > 16384:
            raise KeyValDataStoreException("003", "Invalid Value. Entered value should not exceed 16 KB.")
        self.__add_object(key,value)
        if ttl is not None:
            time.sleep(ttl)
            self.__remove_object(key)


    # ---------------------------------------------------------------------------
    # FUNCTION NAME : read()
    # ---------------------------------------------------------------------------
    # PARAMETERS    : key
    # ---------------------------------------------------------------------------
    # RETURN        : JSON object of entered key
    # ---------------------------------------------------------------------------
    # DESCRIPTION   : Reads file using Linux OS command line tool awk and returns
    #               JSON object of entered key if it exists.
    # ---------------------------------------------------------------------------
    def read(self, key):
        # Check valid key and return JSON object
        key = json.loads(key)
        hash_value = self.__hash_function(key)
        command = "awk 'NR=="+hash_value+"{print $0}' "+self.filepath
        obj = str(os.popen(command=command).read().strip())
        if obj != "":
            obj.split()
            # obj[0] contains key and obj[1] contains value
            return json.dumps(obj[1])
        else:
            raise KeyValDataStoreException("007", "Specified key does not exist.")


    # ---------------------------------------------------------------------------
    # FUNCTION NAME : delete()
    # ---------------------------------------------------------------------------
    # PARAMETERS    : key
    # ---------------------------------------------------------------------------
    # RETURN        : None
    # ---------------------------------------------------------------------------
    # DESCRIPTION   : Calls private function __remove_object utility function
    #               to remove particular line from database file if that line
    #               is not empty.
    # ---------------------------------------------------------------------------
    def delete(self, key):
        self.__remove_object(key)
        