
import serial
import base64


FEND = '7E7E'
def decode_date(date_str):
    return {
            'year': int(date_str[4:8], 16),
            'month': int(date_str[8:10], 16),
            'day': int(date_str[10:12], 16),
            'hour': int(date_str[14:16], 16),
            'minute': int(date_str[16:18], 16),
            'second': int(date_str[18:20], 16),
           }


def decode(txt_buf):
    res = decode_date(txt_buf)
    txt_buf = txt_buf[28:]
    if txt_buf[:2] != '02':
        print("Unknown data", txt_buf[:2])
        return {}
    pkt_type = txt_buf[2:4]
    txt_buf = txt_buf[4:]
    if pkt_type == '01':
        res['Effect'] = int(txt_buf[2:10], 16)
    elif pkt_type in ['09', '0E']:
        res['Version identifier'] = base64.b16decode(txt_buf[4:18]).decode("utf-8")
        txt_buf = txt_buf[18:]
        res['Meter-ID'] = base64.b16decode(txt_buf[4:36]).decode("utf-8")
        txt_buf = txt_buf[36:]
        res['Meter type'] = base64.b16decode(txt_buf[4:20]).decode("utf-8")
        txt_buf = txt_buf[20:]
        res['Effect'] = int(txt_buf[2:10], 16)
        if pkt_type == '0E':
            print('1--', txt_buf)
            txt_buf = txt_buf[10:]
            txt_buf = txt_buf[78:]
            print('2--', txt_buf)
            res['Cumulative_hourly_active_import_energy'] = int(txt_buf[2:10], 16)
            txt_buf = txt_buf[10:]
            print('3--', txt_buf)
            res['Cumulative_hourly_active_export_energy'] = int(txt_buf[2:10], 16)
            txt_buf = txt_buf[10:]
            print(txt_buf)
            res['Cumulative_hourly_reactive_import_energy'] = int(txt_buf[2:10], 16)
            txt_buf = txt_buf[10:]
            print(txt_buf)
            res['Cumulative_hourly_reactive_export_energy'] = int(txt_buf[2:10], 16)
            txt_buf = txt_buf[10:]
    else:
        print("Unknown type", txt_buf[2:4]) 
        return {}

    return res


# async request to ha

# async save to db


if __name__ == '__main__':
    txt_buf = ''
    ser = serial.Serial("/dev/ttyUSB0", baudrate=2400, timeout=0, parity=serial.PARITY_NONE)
    
    while True:
        if ser.inWaiting():
            txt_buf += "".join("{0:02x}".format(x).upper() for x in bytearray(ser.read(200)))
    
            if len(txt_buf) < 6 or not txt_buf[:2] == '7E':
                continue
            pos = txt_buf[2:].find(FEND)
            if pos < 0:
                continue
            print("--", txt_buf)
            current_buf = txt_buf[:pos + 2]
            print(current_buf)
            txt_buf = txt_buf[pos + 4:]
            print(txt_buf)
            print(decode(current_buf[34:]))
            print('\n\n\n')
