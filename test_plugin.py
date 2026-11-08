"""Adapter checks using installed AstrBot API, without external chat sends."""
import importlib
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import AsyncMock

package=types.ModuleType('rabbit_test')
package.__path__=[str(Path(__file__).parent)]
sys.modules['rabbit_test']=package
module=importlib.import_module('rabbit_test.main')
from astrbot.core.star.filter.command import CommandFilter
from astrbot.core.star.filter.regex import RegexFilter
from astrbot.api.message_components import Plain


class Event:
    is_at_or_wake_command=False
    def __init__(self, text='hello', platform='llbot', group='123', sender='42'):
        self.text,self.platform,self.group,self.sender=text,platform,group,sender
        self.stopped=False
    def get_platform_id(self): return self.platform
    def get_group_id(self): return self.group
    def get_sender_id(self): return self.sender
    def get_sender_name(self): return '测试用户'
    def get_self_id(self): return '999'
    def is_private_chat(self): return not self.group
    def get_message_str(self): return self.text
    def get_messages(self): return [Plain(self.text)]
    def stop_event(self): self.stopped=True
    def plain_result(self,text): return text


class Adapter(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.plugin=module.RabbitChatPlugin.__new__(module.RabbitChatPlugin)
        self.plugin.context=types.SimpleNamespace(send_message=AsyncMock(return_value=True))
        cfg=dict(enabled=True,platform_id='llbot',group_id='123',id_namespace='qq',source_id='qq:group:123',admin_ids=['qq:42'])
        self.plugin.bridge=types.SimpleNamespace(config=cfg,ready=True,admin=AsyncMock(),emit=AsyncMock(),consume=lambda:True)

    async def test_actual_command_argument_parser(self):
        command_filter=CommandFilter('rabbit')
        command_filter.init_handler_md(types.SimpleNamespace(handler=module.RabbitChatPlugin.rabbit))
        parsed=command_filter.validate_and_convert_params(['白名单','添加','Steve'],command_filter.handler_params)
        self.assertEqual(parsed,{'arguments':'白名单 添加 Steve'})
        event=Event('/rabbit 白名单 添加 Steve')
        self.assertEqual([item async for item in self.plugin.rabbit(event,**parsed)],[])
        self.assertTrue(event.stopped)
        self.plugin.bridge.admin.assert_awaited_once_with('qq:42',{'action':'add','name':'Steve'})

    async def test_scope_and_loop(self):
        for event in [Event(platform='other'),Event(group='456'),Event(group=''),Event(sender='999')]:
            await self.plugin.group_chat(event)
        self.plugin.bridge.emit.assert_not_awaited()
        await self.plugin.group_chat(Event())
        data=self.plugin.bridge.emit.await_args.args[1]
        self.assertEqual(data['sender']['id'],'qq:42')
        self.assertEqual(data['source']['id'],'qq:group:123')

    async def test_status_reply_before_stop(self):
        event=Event('/rabbit 状态')
        responses=self.plugin.rabbit(event,'状态')
        self.assertEqual(await anext(responses),'RabbitCore：已连接')
        self.assertFalse(event.stopped, 'AstrBot drops yielded replies after stop_event')
        with self.assertRaises(StopAsyncIteration):
            await anext(responses)
        self.assertTrue(event.stopped)

    async def test_name_only_wake_prefix_fallback(self):
        event=Event('/rabbit 状态')
        cfg={'wake_prefix':['薄绿','小薄绿']}
        self.assertFalse(CommandFilter('rabbit').filter(event,cfg))
        self.assertTrue(RegexFilter(r'^/?rabbit(?:\s|$)').filter(event,cfg))
        responses=self.plugin.rabbit_without_wake_prefix(event)
        self.assertEqual(await anext(responses),'RabbitCore：已连接')
        self.assertFalse(event.stopped)
        with self.assertRaises(StopAsyncIteration): await anext(responses)
        self.assertTrue(event.stopped)

    async def test_fallback_does_not_duplicate_standard_command(self):
        event=Event('/rabbit 状态')
        event.is_at_or_wake_command=True
        self.assertEqual([r async for r in self.plugin.rabbit_without_wake_prefix(event)],[])
        self.assertFalse(event.stopped)

    async def test_fallback_scope(self):
        event=Event('/rabbit 状态',group='456')
        self.assertEqual([r async for r in self.plugin.rabbit_without_wake_prefix(event)],[])
        self.assertFalse(event.stopped)

    async def test_command_not_chat(self):
        for text in ['/rabbit 白名单 添加 Steve','/help','白名单 添加 Steve','!help']:
            await self.plugin.group_chat(Event(text))
        self.plugin.bridge.emit.assert_not_awaited()

    async def test_fixed_destination(self):
        await self.plugin.deliver('hello','qq:42')
        self.assertEqual(self.plugin.context.send_message.await_args.args[0],'llbot:GroupMessage:123')


if __name__=='__main__': unittest.main()
