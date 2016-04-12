from .listDict import listDict

def convert_list2UniqueIndexes(
    list_I,column_label_I='list',column_index_I='index'
    ):
    '''convert list to unique indexes
    INPUT:
    list_I = [], np.array
    column_label_I = string, column label for list_I
    column_index_I = string, column label for the index
    OUTPUT:
    indexes_O = np.array of type integer of len(list_I)
    '''
    #list_I = [];
    #column_label_I = '';
    #column_index_I = '';
    indexes_O = [];
    listdict = listDict()
    listdict.set_dictList({column_label_I:list_I})
    listdict.convert_dictList2DataFrame()
    listdict.make_dummyIndexColumn(column_index_I=column_index_I,column_label_I=column_label_I)
    indexes_O = listdict.dataFrame[column_index_I].get_values();
    return indexes_O;