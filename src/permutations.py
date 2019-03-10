def gen_next_permutation(letters, orig_letters, total_chars, arr_len):
    index = arr_len - 1
    while index >= 0:
        if letters[index] != orig_letters[index] - 1:
            letters[index] = letters[index] + 1 if letters[index] < (total_chars - 1) else 0
            break
        letters[index] += 1
        index -= 1
    else:
        return None
    return letters