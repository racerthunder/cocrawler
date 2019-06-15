import aiohttp
import asyncio
import logging
import io
from aiohttp.client_reqrep import ClientRequest
import mimetypes
import os.path as osp
import os
from yarl import URL

logging.basicConfig(level=logging.INFO)


async def on_request_start(session, trace_config_ctx, params):
    print("Starting %s request for %s. I will send: %s" % (params.method, params.url, params.headers))

async def on_request_end(session, trace_config_ctx, params):
    print("Ending %s request for %s. I sent: %s" % (params.method, params.url, params.headers))
    print('Sent headers: %s' % params.response.request_info.headers)

trace_config = aiohttp.TraceConfig()
trace_config.on_request_start.append(on_request_start)
trace_config.on_request_end.append(on_request_end)




attachment = open("/Volumes/crypt/sim.jpg", "rb")

file = (io.BytesIO(attachment.read()), 'sim.jpg')

mailgun_data = {
    "from": "test@test.com",
    "to": "sebastiaan@sevaho.io",
    "subject": "testje",
    "html": "testje",
}


async def main1():

    try:
        async with aiohttp.ClientSession(trace_configs=[trace_config]) as session:
            # MultipartWriter can only be used once, recreate if on retry
            with aiohttp.MultipartWriter('form-data') as mpwriter:

                f, filename = file

                payload = aiohttp.payload.get_payload(f)
                payload.set_content_disposition('form-data',
                                                name='uploaded_file',
                                                filename=filename,
                                                quote_fields=True)
                mpwriter.append_payload(payload)

                for part in mpwriter:
                    print(part)

                url = 'http://bocadoiberico.com/plugins/content/upload.php'
                #url = "http://localhost/upload.php"
                async with session.post(url,
                    data=mpwriter,
                ) as resp:
                    response = await resp.text()
                    print(response)


    except Exception as e:
        print(e)
        raise Exception("something went wrong while sending email")



async def main2():
    mp = aiohttp.MultipartWriter()
    mp.append('hell[')
    async with aiohttp.ClientSession() as session:
        result = await session.post('http://localhost/upload.php', data=mp)

        response = await result.text()
        print(response)





async def main3():

    sample_data = {
        'host': 'localhost',
        'label': 'DefaultNode',

    }

    file = open('/Volumes/crypt/sim.jpg', 'rb')
    filename = 'sim.jpg'
    url = 'http://localhost/upload.php'
    #url = 'http://bocadoiberico.com/plugins/content/upload.php'
    data = aiohttp.FormData()

    f = io.BytesIO(file.read())

    data.add_field('uploaded_file', f, filename=filename, content_type='application/octet-stream')

    for k, v in sample_data.items():
        data.add_field(k, v)



    #headers={'Content-Type': 'application/octet-stream'}
    headers={'Content-Type': 'multipart/form-data'}

    async with aiohttp.ClientSession(trace_configs=[trace_config]) as session:
        result = await session.post(url, data=data)
        response = await result.text(encoding='utf-8')
        print(response)



asyncio.run(main1())


#https://github.com/aio-libs/aiohttp/blob/master/tests/test_multipart.py
