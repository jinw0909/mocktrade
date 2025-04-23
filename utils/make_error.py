

class MakeErrorType:
    def __init__(self) -> None:
        pass

    def change_str(self, string: str):
        try:
            num = float(string)
        except ValueError as e:
            num = 0

        return num

    # Error Select
    def api_connetion_error(self, error_code: int) -> str:
        message = 'OK' if error_code == 0 else (
                  'Server Error' if error_code == 1 else (
                  'Secret-key authentication failed' if error_code == 100 else (
                  'Okx Passphrase Not Found' if error_code == 101 else (
                  'API-key is invalid' if error_code == 102 else (
                  'Registered api' if error_code == 103 else (
                  'Unregistered user' if error_code == 104 else (
                  'Unregistered api' if error_code == 105 else (
        ))))))))

        return message
    
    def error(self, error_code: int) -> str:
        message = 'OK' if error_code == 0 else (
                  'Server Error' if error_code == 1 else (
                  'Not Connet ' if error_code == 100 else (
                  'Not Profile information ' if error_code == 102 else (
                  'check marging type' if error_code == 103 else (
                  'Insufficient amount' if error_code == 104 else (
                  'no user' if error_code == 105 else (
                  'datetime error' if error_code == 106 else (
                  'size error' if error_code == 107 else(
                  'tp_sl_set_ price error' if error_code == 30006 else(
                  'set_ price error' if error_code == 30007 else(
                  'magin_error' if error_code == 30008 else(
                  'sl_error' if error_code == 30009 else(
                  'tp_error' if error_code == 30010 else(
                  'qty_error' if error_code == 30011 else(
                  'Parameter sz error' if error_code == 30012 else(
                  'unknown error' if error_code == 30013 else(
                  'check_position_unknown error' if error_code == 30014 else(
                  'exist_position' if error_code == 30017 else(
                  'user_id error' if error_code == 30022 else(
                  'foc_key error' if error_code == 30023 else(
                  'position_mode error' if error_code == 30024 else(
                  'canceled or does not exist' if error_code == 30028 else(
                  'Position doesnt exist' if error_code == 30029 else(
                  'function error'  if error_code == 30030 else(
                  'Rate limit reached'  if error_code == 30031 else(
                    
                  )
                    
                  )

                  )
                  )

                )

                )

                    )

                    )
                    
                )

                    )

                    )

                    )

                    )

                    )

                    )

                                )
                    )

                  )
                    

                    

                        
        ))))))))

        return message
