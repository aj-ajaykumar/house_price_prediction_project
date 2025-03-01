import os
import sys

class HousingException(Exception):

    def __init__(self, error_message:Exception,error_details:sys):
        super().__init__(error_message)
        self.error_message = HousingException.get_detailed_error_message(error_message=error_message,
                                                                         error_detail=error_details
                                                                         )


    @staticmethod
    def get_detailed_error_message(error_message:Exception,error_detail:sys):
        """
        error_message:Exception object
        error_details:object of sys module
        """

        _,_,exec_tb = error_detail.exc_info()
        exception_block_line = exec_tb.tb_frame.f_lineno
        try_block_lineNumber = exec_tb.tb_lineno
        fileName = exec_tb.tb_frame.f_code.co_filename

        error_message = f"""error occured in script: 
        {[fileName]} at 
        try block line Number: [{try_block_lineNumber}] and exception block line number: [{exception_block_line}],
        error message: [{error_message}]
        """
        return error_message
    
    def __str__(self):  # it is used to  return the details in string ex : print(housingException()) gives the messsage
        return self.error_message()
    
    def __repr__(self): # it is used to represent the object Ex: housingException()----> gives housing exception name
        return HousingException.__name__.str()

