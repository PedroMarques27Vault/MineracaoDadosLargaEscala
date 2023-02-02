import math
import sys

# This algorithm splits the given file into 2
# The second file will then only contain x percentage of the original one
# There is no intersection between records
if __name__ == "__main__":
    params = ["-rf", "-lo"]
    if len(set([x for x in params if x in sys.argv])) !=2:
        print("Incorrect Parameters\n")
        print("Use: \n-rf Filename_of_User_Ratings(String)\n-lo leave_out_percentage_float(Double/Float)")
        sys.exit(-1)
        
        
    for i in range(4):
        if sys.argv[i+1] == "-rf":
            r_filename = sys.argv[i+2]
            
  
        elif sys.argv[i+1] == "-lo":
            leaveout = sys.argv[i+2]
      
    file1 = open(r_filename, 'r')
    lines = file1.readlines()
    no = math.floor(len(lines)*float(leaveout))
    res = lines[:len(lines)-no]
    leave = lines[len(lines)-no:]
    
    
    file2 = open(r_filename, 'w')
    file2.writelines(res)
    file2.close()
    
    
    file3 = open("totest.csv", 'w')
    file3.writelines(leave)
    file3.close()
    
    
