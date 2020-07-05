def input_str_permutation_combo(input_str):
    if len(input_str) == 0:
        return ['']
    prev_list = input_str_permutation_combo(input_str[1:len(input_str)])
    next_list = []
    for i in range(0,len(prev_list)):
        for j in range(0,len(input_str)):
            new_input_str = prev_list[i][0:j]+input_str[0]+prev_list[i][j:len(input_str)-1]
            if new_input_str not in next_list:
                next_list.append(new_input_str)
    return next_list
#print(input_str_permutation_combo("VENKY"))
print(input_str_permutation_combo("ACT"))