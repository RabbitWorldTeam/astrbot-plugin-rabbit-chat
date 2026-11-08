import asyncio
import json
import time
import unittest
import uuid

from aiohttp import ClientSession, WSServerHandshakeError
from bridge import Bridge, command, result_text


class Commands(unittest.TestCase):
    def test_commands(self):
        for verb, action in [('添加','add'),('移除','remove'),('查看','info')]:
            self.assertEqual(command(f'/rabbit 白名单 {verb} Steve'), {'action':action,'name':'Steve'})
        self.assertEqual(command('/rabbit 白名单 暂停 Steve')['enabled'], False)
        self.assertEqual(command('/rabbit 白名单 恢复 Steve')['enabled'], True)
        self.assertEqual(command('/rabbit 白名单 列表 2'), {'action':'list','page':2})
        self.assertEqual(command('/rabbit 状态'), {'action':'connection'})
        self.assertIsNone(command('hello'))
        for raw in ['/rabbit 白名单 off', '/rabbit 白名单 添加 a', '/rabbit 白名单 添加 Steve stop', '/rabbit 白名单 列表 0', '/rabbit 白名单 列表 ²']:
            with self.assertRaises(ValueError):
                command(raw)

    def test_render(self):
        text=result_text({'ok':True,'enabled':True,'entry':{'name':'Steve','enabled':False}})
        self.assertIn('Steve：暂停',text)
        self.assertIn('全局白名单：开启',text)


class Protocol(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.delivered=[]
        async def deliver(text,sender): self.delivered.append((text,sender))
        self.bridge=Bridge(dict(enabled=True,host='127.0.0.1',port=0,token='x'*32,server_id='test',source_id='qq:group:123',admin_ids=['qq:42']),deliver)
        await self.bridge.start()
        port=self.bridge.runner.sites.copy().pop()._server.sockets[0].getsockname()[1]
        self.url=f'http://127.0.0.1:{port}/rabbit/ws'
        self.client=ClientSession()

    async def asyncTearDown(self):
        await self.bridge.stop()
        await self.client.close()

    async def connect(self):
        ws=await self.client.ws_connect(self.url,headers={'Authorization':'Bearer '+'x'*32})
        hello=self.bridge.envelope('hello',{'capabilities':['admin.whitelist']})
        await ws.send_json(hello)
        ready=await ws.receive_json()
        self.assertEqual(ready['data']['reply_to'],hello['id'])
        return ws

    async def test_auth_and_single_connection(self):
        with self.assertRaises(WSServerHandshakeError) as error:
            await self.client.ws_connect(self.url)
        self.assertEqual(error.exception.status,401)
        ws=await self.connect()
        with self.assertRaises(WSServerHandshakeError) as error:
            await self.client.ws_connect(self.url,headers={'Authorization':'Bearer '+'x'*32})
        self.assertEqual(error.exception.status,409)
        await ws.close()

    async def test_admin_route_and_chat(self):
        ws=await self.connect()
        with self.assertRaises(PermissionError):
            await self.bridge.admin('qq:99',{'action':'add','name':'Steve'})
        await self.bridge.admin('qq:42',{'action':'set','name':'Steve','enabled':False})
        request=await ws.receive_json()
        self.assertEqual(request['data']['source']['id'],'qq:group:123')
        self.assertIs(request['data']['enabled'],False)
        data=dict(reply_to=request['id'],source_id='qq:group:999',sender_id='qq:42',action='set',ok=True,message='ok')
        await ws.send_json(self.bridge.envelope('admin.whitelist.result',data))
        await asyncio.sleep(.03)
        self.assertFalse(self.delivered)
        data['source_id']='qq:group:123'
        response=self.bridge.envelope('admin.whitelist.result',data)
        await ws.send_json(response)
        await ws.send_json(response)
        await asyncio.sleep(.03)
        self.assertEqual(len(self.delivered),1)
        self.assertEqual(self.delivered[0][1],'qq:42')
        chat=self.bridge.envelope('chat.game',{'sender':{'name':'Steve'},'text':'hello'})
        await ws.send_json(chat)
        await ws.send_json(chat)
        await asyncio.sleep(.03)
        self.assertEqual(len(self.delivered),2)
        self.assertIn('[MC] Steve：hello',self.delivered[-1][0])
        await self.bridge.emit('chat.external',{'text':'hi'})
        self.assertEqual((await ws.receive_json())['type'],'chat.external')
        await ws.ping()
        await ws.close()

    async def test_disconnect_and_expiry(self):
        ws=await self.connect()
        await self.bridge.admin('qq:42',{'action':'list','page':1})
        await ws.receive_json()
        key=next(iter(self.bridge.pending))
        self.bridge.pending[key]=(time.monotonic()-21,'qq:42','list')
        await asyncio.sleep(1.1)
        self.assertIn('超时',self.delivered[-1][0])
        await self.bridge.admin('qq:42',{'action':'status'})
        await ws.receive_json()
        await ws.close()
        await asyncio.sleep(.03)
        self.assertFalse(self.bridge.pending)
        self.assertIn('断开',self.delivered[-1][0])

    async def test_invalid_envelopes(self):
        for change in [{'version':True},{'server_id':'other'},{'timestamp':0},{'id':'bad'},{'id':42}]:
            raw=self.bridge.envelope('hello',{})
            raw.update(change)
            with self.assertRaises((ValueError,TypeError)):
                self.bridge.validate(json.dumps(raw))


if __name__=='__main__':
    unittest.main()
