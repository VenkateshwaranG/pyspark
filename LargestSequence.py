#from itertools import groupby
#from operator import itemgetter

#data = [1,2,3,2,4,5,6,7,8,1,0,4,5,6]
#new_l = []
#for k, g in groupby(enumerate(data), lambda x : x[0] - x[1]):
#    new_l.append(list(map(itemgetter(1), g)))
#
#print(max(new_l, key=lambda x: len(x)))

def longest_sub_seq(arr):
    #Removing duplicate entrues from the input list.
    #arr = []
   # for i in input_array:
   #     if i not in arr:
   #         arr.append(i)
    main_arr = []
    sub_arr = []
    n = len(arr) #14
    for ind in range(n):
        if ind < n - 1 and arr[ind] < arr[ind+1]:
           sub_arr.append(arr[ind])
        else:
           sub_arr.append(arr[ind])
           main_arr.append(sub_arr)
           sub_arr = []
    return max(main_arr, key=len)

INPUT = [1,2,3,2,4,5,6,7,8,1,0,4,5,6]
print(longest_sub_seq(INPUT))