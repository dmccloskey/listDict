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
    def __init__(self,listDict_I=None):
        self.data=None; # of type list, numpy.array, etc.
        if listDict_I:
            self.listDict=listDict_I;
        else:
            self.listDict=[];

    def add_listDict(self,listDict_I):
        '''add a list of dictionaries'''
        self.listDict = listDict_I;
    def get_listDict(self):
        '''get a list of dictionaries'''
        return self.listDict;
    def get_data(self):
        '''get the data'''
        return self.data;
    def clear_listDict(self):
        '''clear the list of dictionaries'''
        self.listDict = [];
    def clear_data(self):
        '''clear the data'''
        self.data = None;
    def clear_allData(self):
        '''clear the list of dicitonaries and the data'''
        self.clear_listDict();
        self.clear_data();
    def import_listDict_csv(self,filename_I):
        '''import a listDict from .csv
        INPUT:
        filename_I = string, name of the file
        '''
        data = base_importData();
        data.read_csv(filename);
        data.format_data();
        self.add_listDict(data.data);
        data.clear_data();
    def export_listDict_csv(self,filename_O):
        '''export a listDict to .csv
        INPUT:
        filename_O = string, name of the file
        '''
        export = base_exportData(self.listDict);
        export.write_dict2csv(filename_O);
        
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
        data_O = numpy.array of shape(len(row_label_unique),len(column_label_unique))
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
        # initialize the data list
        data_O = self.initialize_dataMatrixList(nrows,ncolumns,na_str_I='NA');
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
                data_tmp = copy(d);
                data_tmp[value_key_name_I]=d[vl];
                data_tmp[value_label_name_I]=vl;
                data_O.append(data_tmp);
        # remove the value_label keys
        for d in data_O:
            for vl in value_labels_I:
                del d[vl]
        return data_O;

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
            dataMatrix_O = numpy.full((nrows_I,ncolumns_I), na_str_I);
        else:
            dataMatrix_O = numpy.zeros((nrows_I,ncolumns_I));
        return dataMatrix_O;

    def extract_arrayFromListDict(self,key_I):
        '''convert a list of dictionary rows to a numpy array
        INPUT:
        key_I = string, dictionary key to extract values from
        OUTPUT:
        data_O = numpy array of values
        
        '''
        data_I = self.listDict;
        data_O = numpy.zeros_like(data_I);
        for i,d in enumerate(data_I):
            data_O[i]=d[key_I];
        return data_O;
