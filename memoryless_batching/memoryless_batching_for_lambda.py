
import copy
import math


def split_into_batches(batch_size, batch_num, split_by):
    new_batch_size = int(batch_size / split_by)
    prev_batch = batch_num - 1
    last_num = split_by * batch_num
    new_batches = range( (prev_batch*split_by)+1, last_num+1 )
    list_for_sqs = []
    for b in new_batches:
        list_for_sqs.append( (new_batch_size, b, split_by) )
    return list_for_sqs



def split_if_large(batch_size, batch_num, split_by, max_events_to_sqs, page_offset):
    final_output=[]
    if batch_size > max_events_to_sqs:
        new_batch_size = int(batch_size / split_by)
        prev_batch = batch_num - 1
        last_num = split_by * batch_num
        new_batches = range( (prev_batch*split_by)+1, last_num+1 )
        x = []
        for b in new_batches:
            x += split_if_large(new_batch_size, b, split_by, max_events_to_sqs, page_offset)
        return x
    else:
        return [(batch_size, batch_num+page_offset, split_by)]



def create_groups(pages, split_by):
    remaining = copy.deepcopy(pages)
    groups_list = []
    while remaining:
        levels = math.floor(math.log(remaining, split_by))
        groups_list.append( levels )
        if levels == 0:
            remaining = 0
        else:
            remaining -= split_by**levels
    return groups_list


def make_final_lists(batch_size, max_events_to_sqs, batch_num, split_by):
    pagenums = batch_size//max_events_to_sqs
    exact_dividend = pagenums*max_events_to_sqs
    remainder = batch_size - exact_dividend
    groups_list = create_groups(pagenums, split_by)
    pages = [split_by**b for b in groups_list]
    batch_sizes = [page*max_events_to_sqs for page in pages]
    offsets = copy.deepcopy(pages)
    offsets.insert(0, 0)
    last_offset = offsets.pop(-1)
    final_output = []
    total_groups = 0
    for num in range(len(batch_sizes)):
        batch_size = batch_sizes[num]
        page_offset = offsets[num]
        temp_output = split_if_large(batch_size=batch_size, 
                                     batch_num=batch_num, 
                                     split_by=split_by,
                                     max_events_to_sqs=max_events_to_sqs,
                                     page_offset=total_groups)
        total_groups += pages[num]
        final_output.extend(temp_output)
    last_page_offset = total_groups
    addendum = split_if_large(batch_size=remainder, 
                              batch_num=batch_num, 
                              split_by=split_by, 
                              max_events_to_sqs=max_events_to_sqs,
                              page_offset=last_page_offset)
    final_output.extend(addendum)
    return final_output



batch_size = 525
max_events_to_sqs = 500
batch_num = 1
split_by = 2

#result = split_if_large(batch_size=exact_dividend, batch_num=1, split_by=2, max_events_to_sqs=20)

new_result = make_final_lists(batch_size=batch_size, 
                              max_events_to_sqs=max_events_to_sqs, 
                              batch_num=batch_num, 
                              split_by=split_by)


for k in new_result:
    print(k)

s = 0
for k in new_result:
    s += k[0]

print(s)

