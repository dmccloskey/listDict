# system
from copy import copy
# Calculate utilities
from .listDict_dependencies import *
# Resources
from io_utilities.base_importData import base_importData
from io_utilities.base_exportData import base_exportData
class listDict():
    '''Utility functions for converting and extracting a list of
    dictionaries into lists and arrays'''
    def __init__(self,listDict_I=None,
            dictList_I=None,
            record_I=None,
            dataFrame_I=None,
            pivotTable_I=None):
        self.data=None; # of type list, nparray, etc.
        if listDict_I:
            self.listDict=listDict_I;
        else:
            self.listDict=[];
        if dictList_I:
            self.dictList=dictList_I;
        else:
            self.dictList={};
        if record_I: #sql record
            self.record=record_I;
        else:
            self.record={};
        if not dataFrame_I is None: #pandas data frame representation
            self.dataFrame=dataFrame_I;
        else:
            self.dataFrame=None;
        if pivotTable_I:#pandas pivot table representation
            self.pivotTable=pivotTable_I;
        else:
            self.pivotTable=None;
        
    def convert_listDict2dataMatrix(self,
                                    row_label_I,column_label_I,value_label_I,
                                    row_variables_I=[],
                                    column_variables_I=[],
                                    data_IO=[],
                                    na_str_I=None,
                                    filter_rows_I=[],
                                    filter_columns_I=[],
                                    order_rows_I=[],
                                    order_columns_I=[],
                                    order_rowsFromTemplate_I=[],
                                    order_columnsFromTemplate_I=[],):
        '''convert a list of dictionary rows to a numpy array
        INPUT:
        data_I = [{}]
        row_label_I = column_id of the row labels
        column_label_I = column_id of the column labels
        value_label_I = column_id of the value label

        OPTIONAL INPUT:
        row_variables_I = list of keys to extract out with the rows
        column_variables_I = list of keys to extract out with the columns
        data_IO = pre-initialized data list
        na_str_I = optional string or value to pre-initialize the output data with
        filter_rows_I = list of row labels to include
        filter_columns_I = list of column labels to include
        order_rows_I = list of integers defining the order of the rows
        order_columns_I = list of integers defining the order of the rows
        order_rowsFromTemplate_I = list of row labels defining the order of the rows
        order_columnsFromTemplate_I = list of row labels defining the order of the rows

        OUTPUT:
        data_O = nparray of shape(len(row_label_unique),len(column_label_unique))
        row_labels_O = row labels of data_O
        column_labels_O = column labels of data_O

        OPTIONAL OUTPUT:
        row_variables_O = {"row_variables_I[0]:[...],..."} where each list is of len(row_labels_O)
        column_variables_O = {"row_variables_I[0]:[...],..."} where each list is of len(column_labels_O)
        '''
        data_O = [];
        data_I = self.listDict;
        # get unique rows and columns
        nrows,row_labels_O = self.get_uniqueValues(row_label_I,filter_I=filter_rows_I);
        ncolumns,column_labels_O = self.get_uniqueValues(column_label_I,filter_I=filter_columns_I);
        # order rows and columns
        row_labels_O,column_labels_O = self.order_rowAndColumnLabels(row_labels_O,column_labels_O,
                order_rows_I=order_rows_I,
                order_columns_I=order_columns_I,
                order_rowsFromTemplate_I=order_rowsFromTemplate_I,
                order_columnsFromTemplate_I=order_columnsFromTemplate_I,
                );
        # initialize the data matrix
        data_O = self.initialize_dataMatrix(nrows,ncolumns,na_str_I);
        # factor
        row_variables_O = {};
        if row_variables_I:
            for cv in row_variables_I:
                row_variables_O[cv]=[];
        column_variables_O = {};
        if column_variables_I:
            for cv in column_variables_I:
                column_variables_O[cv]=[];
        #make the dataMatrixList
        cnt = 0;
        cnt_bool = True;
        cnt2_bool = True;
        for r_cnt,r in enumerate(row_labels_O):
            cnt2_bool = True;
            for c_cnt,c in enumerate(column_labels_O):
                for d in data_I:
                    if d[column_label_I] == c and d[row_label_I] == r:
                        if d[value_label_I]:
                            data_O[r_cnt,c_cnt] = d[value_label_I];
                            if cnt_bool and column_variables_I:
                                for cv in column_variables_I:
                                    column_variables_O[cv].append(d[cv]);
                            if cnt2_bool and row_variables_I:
                                for rv in row_variables_I:
                                    row_variables_O[rv].append(d[rv]);
                                cnt2_bool = False;
                            break;
                cnt = cnt+1
            cnt_bool = False;
        #return output based on input
        if row_variables_I and column_variables_I:
            return data_O,row_labels_O,column_labels_O,row_variables_O,column_variables_O;
        elif row_variables_I:
            return data_O,row_labels_O,column_labels_O,row_variables_O;
        elif column_variables_I:
            return data_O,row_labels_O,column_labels_O,column_variables_O;
        else:
            return data_O,row_labels_O,column_labels_O;
    def convert_listDict2dataMatrixList(self,
                                    row_label_I,column_label_I,value_label_I,
                                    row_variables_I=[],
                                    column_variables_I=[],
                                    data_IO=[],
                                    na_str_I="NA",
                                    order_rows_I=[],
                                    order_columns_I=[],
                                    order_rowsFromTemplate_I=[],
                                    order_columnsFromTemplate_I=[],

                                    ):
        '''convert a list of dictionary rows to a numpy array
        INPUT:
        data_I = [{}]
        row_label_I = column_id of the row labels
        column_label_I = column_id of the column labels
        value_label_I = column_id of the value label

        OPTIONAL INPUT:
        row_variables_I = list of keys to extract out with the rows
        column_variables_I = list of keys to extract out with the columns
        data_IO = pre-initialized data list
        na_str_I = optional string or value to pre-initialize the output data with
        order_rows_I = list of integers defining the order of the rows
        order_columns_I = list of integers defining the order of the rows
        order_rowsFromTemplate_I = list of row labels defining the order of the rows
        order_columnsFromTemplate_I = list of row labels defining the order of the rows

        OUTPUT:
        data_O = list of values ordered according to (len(row_label_unique),len(column_label_unique))
        row_labels_O = row labels of data_O
        column_labels_O = column labels of data_O

        OPTIONAL OUTPUT:
        row_variables_O = {"row_variables_I[0]:[...],..."} where each list is of len(row_labels_O)
        column_variables_O = {"row_variables_I[0]:[...],..."} where each list is of len(column_labels_O)
        '''
        data_O = [];
        data_I = self.listDict;
        # get unique rows and columns
        nrows,row_labels_O = self.get_uniqueValues(row_label_I);
        ncolumns,column_labels_O = self.get_uniqueValues(column_label_I);
        # order rows and columns
        row_labels_O,column_labels_O = self.order_rowAndColumnLabels(row_labels_O,column_labels_O);
        # factor
        row_variables_O = {};
        if row_variables_I:
            for cv in row_variables_I:
                row_variables_O[cv]=[];
        column_variables_O = {};
        if column_variables_I:
            for cv in column_variables_I:
                column_variables_O[cv]=[];
        # initialize the data list
        data_O = self.initialize_dataMatrixList(nrows,ncolumns,na_str_I='NA');
        #make the dataMatrixList
        cnt = 0;
        cnt_bool = True;
        cnt2_bool = True;
        for r in row_labels_O:
            cnt2_bool = True;
            for c in column_labels_O:
                for d in data_I:
                    if d[column_label_I] == c and d[row_label_I] == r:
                        if d[value_label_I]:
                            data_O[cnt] = d[value_label_I];
                            if cnt_bool and column_variables_I:
                                for cv in column_variables_I:
                                    column_variables_O[cv].append(d[cv]);
                            if cnt2_bool and row_variables_I:
                                for rv in row_variables_I:
                                    row_variables_O[rv].append(d[rv]);
                                cnt2_bool = False;
                            break;
                cnt = cnt+1
            cnt_bool = False;
        #return output based on input
        if row_variables_I and column_variables_I:
            return data_O,row_labels_O,column_labels_O,row_variables_O,column_variables_O;
        elif row_variables_I:
            return data_O,row_labels_O,column_labels_O,row_variables_O;
        elif column_variables_I:
            return data_O,row_labels_O,column_labels_O,column_variables_O;
        else:
            return data_O,row_labels_O,column_labels_O;

    def order_rowAndColumnLabels(self,
            row_labels_I,column_labels_I,
            order_rows_I=[],
            order_columns_I=[],
            order_rowsFromTemplate_I=[],
            order_columnsFromTemplate_I=[],):
        '''Order rows and columns according to input
        INPUT:
        row_labels_I = list of unique row labels
        column_labels_I = list of unique column labels
        OUTPUT:
        row_labels_O = list of ordered row labels
        column_labels_O = list of ordered column labels
        '''
        row_labels_O,column_labels_O=row_labels_I,column_labels_I;
        # order the rows and columns
        if order_rows_I:
            row_labels_O = self.order_labels(row_labels_I,order_rows_I);
        if order_columns_I:
            column_labels_O = self.order_labels(column_labels_I,order_columns_I);
        if order_rowsFromTemplate_I:
            row_labels_O = self.order_labelsFromTemplate(row_labels_I,order_rowsFromTemplate_I);
        if order_columnsFromTemplate_I:
            column_labels_O = self.order_labelsFromTemplate(column_labels_I,order_columnsFromTemplate_I);
        return row_labels_O,column_labels_O;

    def get_uniqueValues(self,key_I,filter_I=[]):
        '''get the unique values for a column key
        INPUT:
        key_I = string, column key
        filter_I = list of column values to not include in the output
        OUTPUT:
        nvalues_O = # of values
        uniqueValues_O = list of unique values
        '''
        nvalues_O=0;
        uniqueValues_O=[];

        data_I = self.listDict;
        # get all values
        values = [];
        for d in data_I:
            values.append(d[key_I]);     
        # filter the values
        if filter_I:
            values = [x for x in values if x in filter_I];
        # get the unique values 
        uniqueValues_O = sorted(set(values))
        # count the values
        nvalues_O = len(uniqueValues_O);
        return nvalues_O,uniqueValues_O;
    def order_labels(self,labels_I,order_I):
        '''Order the labels from a pre-defined index
        INPUT:
        labels_I = list of strings
        order_I = list of integers defining the order of the labels
        OUTPUT:
        labels_O = list of ordered strings
        '''
        labels_O = [];
        if len(labels_I)==len(order_I):
            labels_O = [labels_I[i] for i in order_I];
        else:
            print('length of labels and order do not match!');
        return labels_O;
    def order_labelsFromTemplate(self,labels_I,template_I):
        '''Order the labels using a template
        NOTES:
        The template may include values not in the labels
        ASSUMPTIONS:
        The template includes all values found in the labels
        INPUT:
        labels_I = list of strings
        template_I = list of strings
        OUTPUT:
        labels_O = list of ordered strings
        '''
        labels_O = [];
        # make the new template
        template = [];
        if len(template_I)>=len(labels_I):
            template = [i for i in template_I if i in labels_I];
        else:
            print('length of labels is less than the template!');
            return labels_O;
        # order the labels
        if len(template)==len(labels_I):
            labels_O = template;
            #for label in labels_I:
            #    for temp in template:
            #        if label == temp:
            #            labels_O.append(label);
            #            break;
        else:
            print('template does not contain all labels!'); 
        return labels_O;

    def count_missingValues(self,values_I,na_str_I='NA'):
        '''count the number of occurances of a missing value in a list of values
        INPUT:
        values_I = list of numeric values
        na_str_I = string identifier of a missing value
        OUTPUT:
        mv_O = # of missing values
        '''
        mv_O = 0;
        for c in values_I:
            if c==na_str_I:
                mv_O += 1;
        return mv_O;

    def initialize_dataMatrixList(self,nrows_I,ncolumns_I,na_str_I='NA'):
        '''initialize dataMatrixList with missing values
        INPUT:
        nrows_I = int, # of rows of data
        ncolumns_I - int, # of columns of data
        na_str_I = string identifier of a missing value
        OUTPUT:
        dataMatrixList_O = list of na_str_I of length nrows_I*ncolumns_I'''
        dataMatrixList_O = [na_str_I for r in range(nrows_I*ncolumns_I)];
        return dataMatrixList_O;

    def initialize_dataMatrix(self,nrows_I,ncolumns_I,na_str_I='NA'):
        '''initialize dataMatrix with missing values
        INPUT:
        nrows_I = int, # of rows of data
        ncolumns_I - int, # of columns of data
        na_str_I = string identifier of a missing value
        OUTPUT:
        dataMatrixList_O = list of na_str_I of length nrows_I*ncolumns_I'''
        if na_str_I:
            dataMatrix_O = npfull((nrows_I,ncolumns_I), na_str_I);
        else:
            dataMatrix_O = npzeros((nrows_I,ncolumns_I));
        return dataMatrix_O;

    def extract_arrayFromListDict(self,key_I):
        '''convert a list of dictionary rows to a numpy array
        INPUT:
        key_I = string, dictionary key to extract values from
        OUTPUT:
        data_O = numpy array of values
        
        '''
        data_I = self.listDict;
        data_O = np.zeros_like(data_I);
        for i,d in enumerate(data_I):
            data_O[i]=d[key_I];
        return data_O;        

    def convert_listDict2ColumnGroupListDict(self,
                    value_labels_I = [],
                    column_labels_I = [],
                    feature_labels_I = [],
                    na_str_I=None,
                    columnValueConnector_str_I='_',
                    ):
        '''
        Convert a linearized listDict into a listDict with additional column labels that are unique
        values in the group column and filled with values in the values column
        INPUT:
        value_labels_I = [] string, column that will be used to fill
                            the additional columns formed by the unique values
                            in the group column
        column_labels_I = [] string, unique values will form additional column labels
        feature_labels_I = [] string, columns to be included
        OUTPUT:

        ASSUMPTIONS:
        columnValueConnector_str_I is not a substring in any of the value_labels or in the column_label_I

        '''
        data_I = self.listDict;
        #get unique group values
        ncolumns_O,uniqueColumns_O = self.get_uniqueGroups(column_labels_I);
        #get unique feature values
        nfeatures_O,uniqueFeatures_O = self.get_uniqueGroups(feature_labels_I);
        #initialize the columnGroupListDict
        listDict_O,columnValueHeader_O = self.initialize_columnGroupListDict(
                        uniqueFeatures_I = uniqueFeatures_O,
                        uniqueColumns_I = uniqueColumns_O,
                        value_labels_I = value_labels_I,
                        column_labels_I = column_labels_I,
                        feature_labels_I =feature_labels_I,
                        na_str_I=na_str_I,
                        columnValueConnector_str_I=columnValueConnector_str_I,
                        );
        #make the new listDict 
        assert(nfeatures_O==len(listDict_O));
        #for d in data_I:
        #    for cnt_feature,features in enumerate(uniqueFeatures_O):
        #        d_features = {k: d[k] for k in features.keys()};
        #        if d_features == features:
        #            for value in value_labels_I:
        #                key = columnValueConnector_str_I.join([d[k] for k in column_labels_I]);
        #                key += columnValueConnector_str_I + value;
        #                listDict_O[cnt_feature][key] = d[value];
        #            break;
        for d in data_I:
            d_features = {k: d[k] for k in feature_labels_I};
            feature_key = str(d_features.values());
            for value in value_labels_I:
                key = columnValueConnector_str_I.join([d[k] for k in column_labels_I]);
                key += columnValueConnector_str_I + value;
                listDict_O[feature_key][key] = d[value];
        listDict_O = [v for k,v in listDict_O.items()];
        return listDict_O,columnValueHeader_O;

    def initialize_columnGroupListDict(self,
                    uniqueFeatures_I = [],
                    uniqueColumns_I = 'sample_name',
                    value_labels_I = [],
                    column_labels_I = [],
                    feature_labels_I = [],
                    na_str_I='NA',
                    columnValueConnector_str_I='_',
                    ):
        '''
        Convert a linearized listDict into a listDict with additional column labels that are unique
        values in the group column and filled with values in the values column
        INPUT:
        ...
        na_str_I = default string, float, boolean, integer, etc. to fill dictionary values
        columnValueConnector_str_I = string, connector to join the uniqueColumns label with the value_labels
        OUTPUT:

        '''
        # make the dict keys
        dict_keys = [];
        dict_keys.extend(feature_labels_I);
        columnValueHeader_O = [];
        for column in uniqueColumns_I:
            for value in value_labels_I:
                column_str = columnValueConnector_str_I.join([column[k] for k in column_labels_I]);
                column_str += columnValueConnector_str_I + value;
                columnValueHeader_O.append(column_str);
        dict_keys.extend(columnValueHeader_O);
        # make the na_str
        if na_str_I:
            na_str=na_str_I;
        else:
            na_str=0.0;
        # make the initial listDict
        #listDict_O = [{} for i in range(len(uniqueFeatures_I))];
        #for cnt,feature in enumerate(uniqueFeatures_I):
        #    listDict_O[cnt] = copy.copy(feature);
        #    for key in columnValueHeader_O:
        #        listDict_O[cnt][key]=na_str_I;
        listDict_O = {};
        for cnt,feature in enumerate(uniqueFeatures_I):
            feature_key = str(feature.values());
            listDict_O[feature_key] = copy.copy(feature);
            for key in columnValueHeader_O:
                listDict_O[feature_key][key]=na_str_I;
        return listDict_O,columnValueHeader_O;
        
    def get_uniqueGroups(self,keys_I,filter_I=[]):
        '''get the unique values for a group of column keys
        INPUT:
        key_I = string, column key
        TODO:
        filter_I = list of column groups to not include in the output
        OUTPUT:
        ngroups_O = # of groups
        uniqueGroups_O = list of unique groups
        '''
        ngroups_O=0;
        uniqueGroups_O=[];

        data_I = self.listDict;
        # get all groups
        data_subset = [{} for i in range(len(data_I))];
        for cnt,d in enumerate(data_I):
            data_subset[cnt]={k: d[k] for k in keys_I};
        uniqueGroups_O = [dict(tupleized) for tupleized in set(tuple(item.items()) for item in data_subset)]
        # count the groups
        ngroups_O = len(uniqueGroups_O);
        return ngroups_O,uniqueGroups_O;

    #Data adders
    def append_dataFrame(self,dataFrame_I):
        '''
        add a new data set of rows to the dataframe
        INPUT:
        dataFrame_I = dataFrame
        '''
        self.dataFrame=self.dataFrame.append(dataFrame_I);
    def append_listDict2dataFrame(self,listDict_I):
        '''
        add a new data set of rows to the dataframe
        INPUT:
        listDict_I = listDict
        '''
        df = pd.DataFrame(listDict_I);
        self.dataFrame=self.dataFrame.append(df);
    def add_column2DataFrame(self,column_label_I,data_I):
        '''
        add a new data column to the dataFrame
        INPUT:
        column_label_I = string
        data_I = [] or numpy.array
        '''
        if self.dataFrame is None:
            series = pd.Series(data=data_I);
            self.dataFrame = series.to_frame(name=column_label_I);
        else:
            self.dataFrame.loc[:,column_label_I] = pd.Series(data_I, index=self.dataFrame.index);

    #Data reset methods
    def clear_listDict(self):
        '''clear the list of dictionaries'''
        self.listDict = [];
    def clear_dictList(self):
        '''clear the dictionary of lists'''
        self.dictList = None;
    def clear_dataFrame(self):
        '''clear the dataFrame'''
        self.dataFrame = None;
    def clear_pivotTable(self):
        '''clear the pivotTable'''
        self.pivotTable = None;
    def clear_allData(self):
        '''clear the list of dicitonaries and the data'''
        self.clear_listDict();
        self.clear_dictList();
        self.clear_dataFrame();
        self.clear_pivotTable();

    #Data io:
    def import_listDict_csv(self,filename_I):
        '''import a listDict from .csv
        INPUT:
        filename_I = string, name of the file
        '''
        data = base_importData();
        data.read_csv(filename_I);
        data.format_data();
        self.set_listDict(data.data);
        #data.clear_data();
    def export_listDict_csv(self,filename_O):
        '''export a listDict to .csv
        INPUT:
        filename_O = string, name of the file
        '''
        export = base_exportData(self.listDict);
        export.write_dict2csv(filename_O);

    #Setters
    def set_listDict(self,listDict_I):
        '''add a list of dictionaries'''
        self.listDict = listDict_I;
    def set_dictList(self,dictList_I):
        '''add a dictionary of lists'''
        self.dictList = dictList_I;
    def set_record(self,record_I):
        '''add a sql record'''
        self.record = record_I;
    def set_dataFrame(self,dataFrame_I):
        '''make a pandas dataFrame from listDict'''
        self.dataFrame=dataFrame_I;
    def set_pivotTable(self,value_label_I,row_labels_I,column_labels_I):
        '''make a pandas pivot_table from a pandas dataFrame'''
        self.pivotTable = self.dataFrame.pivot_table(
            values=value_label_I,
            index = row_labels_I,
            columns = column_labels_I);

    #Getters
    def get_listDict(self):
        '''get a list of dictionaries'''
        return self.listDict;
    def get_dictList(self):
        '''get a dictionary of lists'''
        return self.dictList;
    def get_record(self):
        '''get a sql record'''
        return self.record;
    def get_dataFrame(self):
        '''return dataFrame
        '''
        return self.dataFrame;
    def get_pivotTable(self):
        '''
        return pivot table
        '''
        return self.pivotTable;

    #Attribute conversions
    def convert_listDict2DataFrame(self):
        '''make a pandas dataFrame from listDict'''
        self.dataFrame = pd.DataFrame(self.listDict);
    def convert_record2DataFrame(self):
        '''make a pandas dataFrame from a sql record'''
        self.dataFrame = pd.DataFrame.from_records(self.record,columns=self.record[0].keys());
    def convert_dictList2DataFrame(self):
        '''make a pandas dataFrame from listDict'''
        self.dataFrame = pd.DataFrame.from_dict(self.dictList);
    def convert_dataFrame2ListDict(self):
        ''' 
        convert a dataFrame to a list of dictionaries
        INPUT:
        OUTPUT:
        row_O = {}
        .where((pd.notnull(df)), None)
        '''
        self.listDict=self.dataFrame.where((pd.notnull(self.dataFrame)), None).to_dict('records');
    def convert_dataFrame2DictList(self):
        ''' 
        convert a dataFrame to a dictionaries of lists
        INPUT:
        OUTPUT:
        row_O = {}
        .where((pd.notnull(df)), None)
        '''
        dictList = {};
        columns = self.dataFrame.columns.get_values();
        for col in columns:
            dictList[col]=self.dataFrame[col].get_values();
        self.dictList=dictList;
    def convert_listDict2ListDictValues(self,
                    value_key_name_I = 'value',
                    value_label_name_I = 'label',
                    value_labels_I=['var_proportion','var_cumulative']):
        '''linearize a list of dictionaries by seriesLabels
        to a linearized version for a multiseries bi plot

        INPUT:
        value_labels_I = list of table columns to use as individual values
        OUTPUT:
        data_O = list of dictionaries of len(listDict)*len(value_labels_I)
            with additional keys "value" = value of value_labels_I[i]
                                 "value_label" = value_labels_I[i]
        '''
        data_I = self.listDict;
        data_O = [];
        # make the linearized list
        for d in data_I: #iterate through the original copy
            for vl in value_labels_I:
                data_tmp = copy.copy(d);
                data_tmp[value_key_name_I]=d[vl];
                data_tmp[value_label_name_I]=vl;
                data_O.append(data_tmp);
        # remove the value_label keys
        for d in data_O:
            for vl in value_labels_I:
                del d[vl]
        return data_O;

    #Getters (specific to an attribute)
    def get_dataMatrixList(self,na_str_I = None):
        '''return a flattened list matrix representation of a pandas pivot_table
        NOTES: this is particularly useful prior to input to R due to the use of NA
        INPUT:
        OPTIONAL INPUT:
        na_str_I = optional string or value to fill missing data with
        '''
        #fill values with 'NA', convert to 1d numpy array, convert to list
        if na_str_I:
            data_O = list(self.pivotTable.fillna(na_str_I).get_values().ravel());
        else:
            data_O = list(self.pivotTable.get_values().ravel())
        return data_O;
    def get_dataMatrix(self,na_str_I = None):
        '''return a matrix representation of a pandas pivot_table
        INPUT:
        OPTIONAL INPUT:
        na_str_I = optional string or value to fill missing data with
        '''
        #fill values with 'NA', convert to 1d numpy array, convert to list
        if na_str_I:
            data_O = self.pivotTable.fillna(na_str_I).get_values();
        else:
            data_O = self.pivotTable.get_values();
        return data_O;
    def get_rowLabels(self,row_labels_I):
        '''return a dictionary of row labels in a pandas pivot_table
        INPUT:
        row_labels_I = list of row labels to extract
        NOTES:
        row_labels_I should consist of ALL row labels in the pandas index tuple
        (labels are extracted by order and NOT by the label value)
        '''
        row_labels_O = {}
        for i,rv in enumerate(row_labels_I):
            row_labels_O[rv] = np.array([g[i] for g in self.pivotTable.index.unique()]);
            #for g in self.pivotTable.index.unique():
            #    row_labels_O[rv].append(g[i]);
        return row_labels_O;
    def get_rowLabels_asTupleArray(self):
        '''return an array of row labels in a pandas pivot_table
        NOTES:
        labels are in the same order as the pivot table was instanciated
        '''
        row_labels_O = self.pivotTable.index.unique();
        return row_labels_O;
    def get_rowLabels_asArray(self):
        '''return an array of row labels in a pandas pivot_table
        NOTES:
        labels are in the same order as the pivot table was instanciated
        '''
        row_labels_O = np.array([np.asarray(row) for row in self.pivotTable.index.unique()]);
        return row_labels_O;
    def get_rowLabels_asDataFrame(self):
        '''return a DataFrame of row labels in a pandas pivot_table
        NOTES:
        labels are in the same order as the pivot table was instanciated
        '''
        row_labels_O = pd.DataFrame.from_records(list(self.pivotTable.index.unique()),columns=self.pivotTable.index.names);
        return row_labels_O;
    def get_rowLabels_asUniqueIndexes(self):
        '''return an array from 0 to nunique indexes in a pandas pivot_table
        NOTES:
        labels are in the same order as the pivot table was instanciated
        '''
        nrow_labels_O = np.array(range(self.pivotTable.index.nunique()));
        return nrow_labels_O;
    def get_columnLabels(self,column_labels_I):
        '''return a dictionary of column labels in a pandas pivot_table
        INPUT:
        column_labels_I = list of column labels to extract
        NOTES:
        column_labels_I should consist of ALL row labels in the pandas columns tuple
        (labels are extracted by order and NOT by the label value)
        '''
        column_labels_O = {}
        for i,cv in enumerate(column_labels_I):
            column_labels_O[cv] = np.array([g[i] for g in self.pivotTable.columns.unique()]);
        return column_labels_O;
    def get_columnLabels_asTupleArray(self):
        '''return a array of column labels in a pandas pivot_table
        NOTES:
        labels are in the same order as the pivot table was instanciated
        '''
        column_labels_O = self.pivotTable.columns.unique();
        return column_labels_O;
    def get_columnLabels_asArray(self):
        '''return a array of column labels in a pandas pivot_table
        NOTES:
        labels are in the same order as the pivot table was instanciated
        '''
        column_labels_O = np.array([np.asarray(col) for col in self.pivotTable.columns.unique()]);
        return column_labels_O;
    def get_columnLabels_asDataFrame(self):
        '''return a array of column labels in a pandas pivot_table
        NOTES:
        labels are in the same order as the pivot table was instanciated
        '''
        column_labels_O = pd.DataFrame.from_records(list(self.pivotTable.columns.unique()),columns=self.pivotTable.columns.names);
        return column_labels_O;
    def get_columnLabels_asUniqueIndexes(self):
        '''return a array from 0 to nunique column labels in a pandas pivot_table
        NOTES:
        labels are in the same order as the pivot table was instanciated
        '''
        column_labels_O = np.array(range(self.pivotTable.columns.nunique()));
        return column_labels_O;
    def get_dataFrameRow_byIndex_asDict(self,row_index_I):
        '''return a dictionary of a row index
        INPUT:
        row_index_I = integer
        OUTPUT:
        row_O = {}
        '''
        row_O = dict(self.dataFrame.iloc[0]);
        return row_O;
    def get_flattenedDataAndColumnLabels(self):
        '''
        return a flattened list of data and corresponding column labels
        EXAMPLE:
        dataFrame:
        A B
        0 1
        2 3
        output:
        data_O = [0,1,2,3];
        column_labeles_O = ['A','B','A','B']

        OUTPUT:
        data_O = flattened array of data
        column_labels_O = flattened array of corresponding column labels
        '''
        stacked = self.dataFrame.stack();
        data_O = stacked.get_values();
        columnLabels_df = pd.DataFrame(list(stacked.index))
        column_labels_O = columnLabels_df[1].get_values();
        return data_O,column_labels_O
    def get_uniqueValues(self,column_I):
        '''return unique values from a dataFrame column
        INPUT:
        column_I = string
        '''
        return self.dataFrame[column_I].unique();

    #Sorting methods
    def order_indexFromTemplate_pivotTable(self,template_I,axis_I):
        '''re-order a column/row from a template
        INPUT:
        template_I = [], strings listing the labels in the desired order
        axis_I = integer, 0=rows, 1=columns
        OUTPUT:
        '''
        mi = pd.Index(template_I);
        self.pivotTable.reindex_axis(mi,axis_I);

    #Filter methods
    def filterIn_byDictList(self,dictList_I):
        '''filter in data that is in a list
        INPUT:
        dictList_I = {'column_label':[items to filter in...]}
        '''
        for k,v in dictList_I.items():
            if v:
                self.dataFrame = self.dataFrame[self.dataFrame[k].isin(v)];
    def filterOut_byDictList(self,dictList_I):
        '''filter out data that is not in a list
        INPUT:
        dictList_I = {'column_label':[items to filter out...]}
        '''
        for k,v in dictList_I.items():
            if v:
                self.dataFrame = self.dataFrame[~self.dataFrame[k].isin(v)];

    #Utility methods
    def count_missingValues_pivotTable(self):
        '''count the number of occurances of a missing value in a pandas pivot table
        INPUT:
        OUTPUT:
        mv_O = # of missing values
        '''
        mv_O = 0;
        #check for missing values
        mv_O = self.pivotTable.size - self.pivotTable.count().get_values().sum();
        return mv_O;
    def get_uniqueValues_list(self,list_I):
        '''retrieve unique values in a list in order
        INPUT:
        list_I = list of values
        '''
        return pd.Series(list_I).unique();
    def convert_list2Levels(self,list_I):
        '''Convert a list of strings to unique indexes'''

        s = pd.Series(list_I)
        levels, labels = pd.factorize(s)
        return levels;

    def make_concatenatedColumn(self,
            column_label_new_I,
            column_labels_I,
            connector_str_I = '__'
            ):

        '''
        make a concatenated column from two columns
        INPUT:
        column_label_new_I = string, new column label
        column_labels_I = [], list of column labels to concatenate
        connector_str_I = string, string to connect the two columns by
        '''
        for i,column_label in enumerate(column_labels_I):
            if i==0:
                self.dataFrame[column_label_new_I] = self.dataFrame[column_label];
            else:
                self.dataFrame[column_label_new_I] = self.dataFrame[column_label_new_I].map(str) + connector_str_I + self.dataFrame[column_label].map(str);
    #def split_concatenatedColumns(self,
    #        column_label_I,
    #        connector_str_I = '__'
    #        ):
    #    '''
    #    split a concatenated column into indiviual columns
    #    '''
    #    column_labels = column_label_I.split(connector_str_I);
    #    self.dataFrame['new_col1'], self.dataFrame['new_col2'] = zip(*df['original_col'].apply(lambda x: x.split(': ', 1)))
    def change_rowAndColumnNames(self,row_names_dict_I=None,column_names_dict_I=None):
        '''
        change the names of multiple columns
        INPUT:
        row_names_dict_I = {}, k,v = old:new row label
        column_names_dict_I = {}, k,v = old:new column label
        '''
        #Rename several DataFrame columns
        self.dataFrame = self.dataFrame.rename(
            index = row_names_dict_I,
            columns = column_names_dict_I
            );
    def make_dummyIndexColumn(self,column_index_I,column_label_I,col2index_I=None):
        '''
        Create a dummy index column based on the unique values in the specific column
        INPUT:
        column_index_I = string, name of the index column
        column_label_I = string, name of the column to create the dummy label
        '''
        if col2index_I is None:
            unique = self.dataFrame[column_label_I].unique();
            unique.sort();
            col2index = {v:i for i,v in enumerate(unique)}
        else:
            col2index = col2index_I;
        index = [col2index[v] for v in self.dataFrame[column_label_I].get_values()];
        self.add_column2DataFrame(column_index_I,index);

    def convert_dictListListDict2ListDict(self,data_I):
        '''flatten a dictionary of listDicts to a listDict
        {key:[{},...],...} -> [[{},...],...] -> [{},...]
        NOTES:
        the 'key' is lost
        '''
        data1 = [v for v in data_I.values()];
        data2=[];
        for d in data1:
            data2.extend(d);
        return data2;

    #Bulk methods
    def convert_listDict2dataMatrixList_pd(self,
                row_label_I,column_label_I,value_label_I,
                row_variables_I=[],
                column_variables_I=[],
                na_str_I="NA",
                filter_rows_I=[],
                filter_columns_I=[],
                order_rowsFromTemplate_I=[],
                order_columnsFromTemplate_I=[],
                                    ):
        '''convert a list of dictionary rows to a numpy array
        INPUT:
        data_I = [{}]
        row_label_I = column_id of the row labels
        column_label_I = column_id of the column labels
        value_label_I = column_id of the value label

        OPTIONAL INPUT:
        row_variables_I = list of keys to extract out with the rows
        column_variables_I = list of keys to extract out with the columns
        na_str_I = optional string or value to pre-initialize the output data with
        filter_rows_I = list of row labels to include
        filter_columns_I = list of column labels to include
        order_rows_I = list of integers defining the order of the rows
        order_columns_I = list of integers defining the order of the rows
        order_rowsFromTemplate_I = list of row labels defining the order of the rows
        order_columnsFromTemplate_I = list of row labels defining the order of the rows

        OUTPUT:
        data_O = list of values ordered according to (len(row_label_unique),len(column_label_unique))
        row_labels_O = row labels of data_O
        column_labels_O = column labels of data_O

        OPTIONAL OUTPUT:
        row_variables_O = {"row_variables_I[0]:[...],..."} where each list is of len(row_labels_O)
        column_variables_O = {"row_variables_I[0]:[...],..."} where each list is of len(column_labels_O)
        '''

        data_O = [];

        #handle the input to pandas
        row_variables = row_variables_I;
        row_variables.insert(0,row_label_I);
        column_variables = column_variables_I;
        column_variables.insert(0,column_label_I);

        #make the pandas dataframe
        self.convert_listDict2DataFrame();

        #filter in rows/columns
        if filter_rows_I:
            data_listDict.filterIn_byDictList({row_label_I:filter_rows_I,
                                           });
        if filter_columns_I:
            data_listDict.filterIn_byDictList({column_label_I:filter_columns_I,
                                           });
        #set the pivot table
        self.set_pivotTable(value_label_I, row_variables, column_variables);

        #sort rows/columns
        if order_rowsFromTemplate_I:
            data_listDict.order_indexFromTemplate_pivotTable(template_I=order_rowsFromTemplate_I,axis_I=0);
        if order_columnsFromTemplate_I:
            data_listDict.order_indexFromTemplate_pivotTable(template_I=order_columnsFromTemplate_I,axis_I=0);
        
        #fill values with 'NA', convert to 1d numpy array, convert to list
        data_O = self.get_dataMatrixList(na_str_I);

        #extract out rows and column variables
        row_variables_O = self.get_rowLabels(row_variables_I);
        row_labels_O = row_variables_O[row_label_I];

        # columns are in the same order as they were initialized during the pivot
        column_variables_O = self.get_columnLabels(column_variables_I);
        column_labels_O = column_variables_O[column_label_I];

        # check that the length of the column_labels and row_labels 
        # are == to what they should be if only the row_label/column_label were used
        assert(len(self.dataFrame.groupby([row_label_I]))==len(row_labels_O));
        assert(len(self.dataFrame.groupby([column_label_I]))==len(column_labels_O));
        ##Broken (works only if len(column_variables/row_variables)==1
        #assert(self.pivotTable.groupby([row_label_I]).count()==len(row_labels_O));
        #assert(self.pivotTable.groupby([column_label_I]).count()==len(column_labels_O));

        #return output based on input
        if row_variables_I and column_variables_I:
            return data_O,row_labels_O,column_labels_O,row_variables_O,column_variables_O;
        elif row_variables_I:
            return data_O,row_labels_O,column_labels_O,row_variables_O;
        elif column_variables_I:
            return data_O,row_labels_O,column_labels_O,column_variables_O;
        else:
            return data_O,row_labels_O,column_labels_O;

