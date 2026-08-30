import asyncio
import json
import time
from typing import List
from datetime import datetime, timedelta

import httpx
from websockets.asyncio.client import connect

from astrbot.api import logger, AstrBotConfig
from astrbot.api.event import filter, AstrMessageEvent, MessageChain
from astrbot.api.star import Context, Star, register
import astrbot.api.message_components as Comp


@register(
    "astrbot_plugin_ret2shell",
    "decimo",
    "Ret2Shell 赛事事件推送插件",
    "1.1.1",
    repo="https://github.com/xiaochai-123/astrbot_plugin_ret2shell"
)
class Ret2ShellPlugin(Star):
    def __init__(self, context: Context, config: AstrBotConfig = None):
        super().__init__(context)

        self.config = config or {}
        self.ws_url = self.config.get("ret2shell_ws_link", "")
        self.public_umo = self._normalize_list(self.config.get("public_umo", []))
        self.admin_umo = self._normalize_list(self.config.get("admin_umo", []))
        self.ops_umo = self._normalize_list(self.config.get("ops_umo", []))

        # 读取事件开关配置，默认全部开启
        default_events = [
            "challenge_up", "challenge_down", "new_hint",
            "blood_1", "blood_2", "blood_3",
            "correct", "cheated", "too_quick",
            "new_notification", "freeze", "unfreeze",
            "cluster_overloaded", "cluster_recovered", "server_panic"
        ]
        self.enabled_events = self.config.get("enabled_events", default_events)
        if not self.enabled_events:
            self.enabled_events = default_events

        self.client = None
        self.api_base_url = ""
        self.game_id = ""

        self.ws_task = None
        self.running = False
        self._game_start_time = None
        self._game_end_time = None

        logger.info(f"🔍 ws_url 已配置: {bool(self.ws_url)}")
        logger.info(f"📢 public_umo: {self.public_umo}")
        logger.info(f"🔔 admin_umo: {self.admin_umo}")
        logger.info(f"🔧 ops_umo: {self.ops_umo}")
        logger.info(f"🔘 已开启事件: {len(self.enabled_events)} 项")

    def _normalize_list(self, value) -> List[str]:
        if isinstance(value, str):
            if "," in value:
                return [v.strip() for v in value.split(",") if v.strip()]
            return [value] if value.strip() else []
        if isinstance(value, list):
            return [str(v).strip() for v in value if str(v).strip()]
        return []

    async def initialize(self):
        logger.info("🚀 Ret2Shell 插件开始加载...")

        if not self.ws_url:
            logger.error("❌ ret2shell_ws_link 未配置")
            return

        await self._init_http_api()

        await self._schedule_game_timers()

        self.running = True
        self.ws_task = asyncio.create_task(self._ws_loop())
        logger.info("✅ WebSocket 任务已启动")

    # ============ HTTP API ============

    async def _init_http_api(self):
        from urllib.parse import urlparse, parse_qsl

        ws_link = self.ws_url
        urlparsed = urlparse(ws_link)
        scheme = urlparsed.scheme
        socket = urlparsed.netloc

        self.api_base_url = f'{"https" if scheme == "wss" else "http"}://{socket}/api'
        self.client = httpx.AsyncClient(
            base_url=self.api_base_url,
            headers={"User-Agent": "astrbot-ret2shell/1.0.0"}
        )
        self.game_id = dict(parse_qsl(urlparsed.query)).get("game_id") or ""

        logger.info(f"🎯 HTTP API: {self.api_base_url}, game_id: {self.game_id}")

    async def _http_get(self, path: str):
        if not self.client or not self.game_id:
            return None
        try:
            resp = await self.client.get(path)
            if resp.status_code == 200:
                return resp.json()
            return None
        except Exception as e:
            logger.error(f"HTTP 请求失败: {e}")
            return None

    # ============ 定时播报 ============

    async def _schedule_game_timers(self):
        """获取赛事起止时间，安排定时播报"""
        try:
            data = await self._http_get(f"/game/{self.game_id}")
            if data:
                start_at = data.get("start_at")
                end_at = data.get("end_at")
                if start_at:
                    self._game_start_time = datetime.fromtimestamp(start_at)
                    logger.info(f"📅 比赛开始时间: {self._game_start_time}")
                    delay = start_at - time.time()
                    if delay > 0:
                        asyncio.create_task(self._delayed_game_start(delay))
                if end_at:
                    self._game_end_time = datetime.fromtimestamp(end_at)
                    logger.info(f"📅 比赛结束时间: {self._game_end_time}")
                    delay = end_at - time.time()
                    if delay > 0:
                        asyncio.create_task(self._delayed_game_end(delay))
        except Exception as e:
            logger.error(f"获取赛事时间失败: {e}")

    async def _get_game_name(self) -> str:
        """获取赛事名称"""
        try:
            data = await self._http_get(f"/game/{self.game_id}")
            if data:
                return data.get("name", "Ret2Shell 赛事")
        except Exception:
            pass
        return "Ret2Shell 赛事"

    async def _delayed_game_start(self, delay: float):
        await asyncio.sleep(delay)
        game_name = await self._get_game_name()
        await self._broadcast_message(f"🏁 【{game_name}】比赛已开始！")

    async def _delayed_game_end(self, delay: float):
        await asyncio.sleep(delay)
        game_name = await self._get_game_name()
        await self._broadcast_message(f"🎉 【{game_name}】比赛已结束！")

    async def _broadcast_message(self, message: str):
        if not self.public_umo:
            return
        for umo in self.public_umo:
            try:
                message_chain = MessageChain().message(message)
                await self.context.send_message(umo, message_chain)
                logger.info(f"✅ 定时播报已推送到 {umo}")
            except Exception as e:
                logger.error(f"❌ 定时播报推送到 {umo} 失败: {e}")

    # ============ WebSocket ============

    async def _ws_loop(self):
        while self.running:
            try:
                logger.info(f"🔌 正在连接 Ret2Shell: {self.ws_url[:60]}...")
                async with connect(self.ws_url) as websocket:
                    logger.info("✅ Ret2Shell WebSocket 连接成功！")
                    async for raw_message in websocket:
                        await self._handle_message(raw_message)
            except asyncio.CancelledError:
                logger.info("⏹️ WebSocket 任务被取消")
                break
            except Exception as e:
                logger.error(f"❌ WebSocket 连接断开: {e}")
                logger.info("⏳ 5秒后重连...")
                await asyncio.sleep(5)

    async def _handle_message(self, raw_message: str):
        try:
            data = json.loads(raw_message)
            logger.info(f"📨 收到原始消息: {raw_message}") 
            event_kind = next(iter(data.keys())) if data else "unknown"
            event_data = data.get(event_kind, {})

            # 提取事件类型标识符，检查是否开启
            event_type_key = self._get_event_type(event_kind, event_data)
            if event_type_key not in self.enabled_events:
                logger.debug(f"⏭️ 事件 {event_type_key} 已关闭，跳过推送")
                return

            msg, msg_type = await self._format_event_message(event_kind, event_data)

            if msg_type == "public":
                targets = self.public_umo
            elif msg_type == "admin":
                targets = self.admin_umo
            elif msg_type == "ops":
                targets = self.ops_umo
            else:
                targets = []

            if targets and msg:
                for umo in targets:
                    try:
                        message_chain = MessageChain().message(msg)
                        await self.context.send_message(umo, message_chain)
                        logger.info(f"✅ [{msg_type}] 消息已推送到 {umo}")
                    except Exception as e:
                        logger.error(f"❌ 推送到 {umo} 失败: {e}")

        except json.JSONDecodeError:
            logger.warning(f"⚠️ 无法解析 JSON: {raw_message[:100]}")
        except Exception as e:
            logger.error(f"❌ 处理消息失败: {e}")

    def _get_event_type(self, event_kind: str, event_data: dict) -> str:
        """提取事件类型标识符，用于开关判断"""
        if event_kind == "challenge":
            event_type = event_data.get("event_type", "unknown")
            if event_type == "up":
                return "challenge_up"
            elif event_type == "down":
                return "challenge_down"
            elif event_type == "new_hint":
                return "new_hint"
            return event_type
        elif event_kind == "submission":
            event_type = event_data.get("event_type", "unknown")
            if event_type == "correct":
                blood_state = event_data.get("blood_state")
                if blood_state == 1:
                    return "blood_1"
                elif blood_state == 2:
                    return "blood_2"
                elif blood_state == 3:
                    return "blood_3"
                return "correct"
            return event_type
        elif event_kind == "game":
            return event_data.get("event_type", "unknown")
        elif event_kind == "devops":
            return event_data.get("event_type", "unknown")
        return "unknown"

    async def _format_event_message(self, event_kind: str, event_data: dict):
        """格式化事件消息，返回 (消息内容, 消息类型)"""
        msg_type = "public"

        game_name = "Ret2Shell 赛事"
        if self.game_id:
            try:
                game_data = await self._http_get(f"/game/{self.game_id}")
                if game_data:
                    game_name = game_data.get("name", "Ret2Shell 赛事")
            except Exception as e:
                logger.debug(f"获取赛事名称失败: {e}")

        lines = [f"🎯 【{game_name}】"]

        if event_kind == "challenge":
            challenge = event_data.get("challenge", {})
            operator = event_data.get("operator", {})
            event_type = event_data.get("event_type", "unknown")
            tag_name = challenge.get("tag", [{}])[0].get("name", "未知")
            challenge_name = challenge.get("name", "未知")

            if event_type == "up":
                lines.append(f"⬆️ [{tag_name}] 新题目上线：{challenge_name}")
            elif event_type == "down":
                lines.append(f"⬇️ [{tag_name}] 题目下线：{challenge_name}")
            elif event_type == "new_hint":
                lines.append(f"💡 [{tag_name}] {challenge_name} 发布了新提示")

        elif event_kind == "submission":
            team = event_data.get("team", {})
            challenge = event_data.get("challenge", {})
            event_type = event_data.get("event_type", "unknown")
            blood_state = event_data.get("blood_state")
            tag_name = challenge.get("tag", [{}])[0].get("name", "未知")
            challenge_name = challenge.get("name", "未知")
            team_name = team.get("name", "未知")

            if event_type == "correct":
                if blood_state == 1:
                    lines.append(f"🎉 恭喜 {team_name} 获得 [{tag_name}] {challenge_name} 一血！🥇")
                    msg_type = "public"
                elif blood_state == 2:
                    lines.append(f"🎉 恭喜 {team_name} 获得 [{tag_name}] {challenge_name} 二血！🥈")
                    msg_type = "public"
                elif blood_state == 3:
                    lines.append(f"🎉 恭喜 {team_name} 获得 [{tag_name}] {challenge_name} 三血！🥉")
                    msg_type = "public"
                else:
                    lines.append(f"✅ {team_name} 解出了 [{tag_name}] {challenge_name}")
                    msg_type = "admin"

            elif event_type == "cheated":
                peer_team = event_data.get("peer_team", {})
                peer_team_name = peer_team.get("name", "未知队伍")
                lines = [f"⚠️ 【{game_name}】检测到作弊行为！！!⚠️"]
                lines.append(f"🤥 {team_name} 提交了 {peer_team_name} 的 [{tag_name}] {challenge_name} 题目的 flag")
                msg_type = "admin"

        elif event_kind == "game":
            event_type = event_data.get("event_type", "unknown")
            if event_type == "new_notification":
                lines.append(f"📢 新通知：{event_data.get('message', '')}")
            elif event_type == "freeze":
                lines.append(f"🧊 比赛已冻结")
            elif event_type == "unfreeze":
                lines.append(f"🌊 比赛已解冻")

        elif event_kind == "chat":
            team = event_data.get("team", {})
            challenge = event_data.get("challenge", {})
            content = event_data.get("content", "")
            tag_name = challenge.get("tag", [{}])[0].get("name", "未知")
            challenge_name = challenge.get("name", "未知")
            team_name = team.get("name", "未知")
            lines.append(f"✉️ {team_name} 对 [{tag_name}] {challenge_name} 发送了反馈：")
            lines.append(f"{content[:200]}")

        elif event_kind == "devops":
            event_type = event_data.get("event_type", "unknown")
            if event_type == "cluster_overloaded":
                lines.append(f"⚠️ 集群超载")
            elif event_type == "cluster_recovered":
                lines.append(f"✅ 集群恢复")
            elif event_type == "server_panic":
                lines.append(f"🔥 服务崩溃")
            msg_type = "ops"

        else:
            lines.append(f"📌 类型: {event_kind}")
            lines.append(json.dumps(event_data, ensure_ascii=False, indent=2)[:300])

        return "\n".join(lines), msg_type

    # ============ 指令 ============

    @filter.command("game")
    async def query_game(self, event: AstrMessageEvent):
        if not self.game_id:
            yield event.plain_result("❌ 未配置有效的 WebSocket 链接")
            return

        data = await self._http_get(f"/game/{self.game_id}")
        if not data:
            yield event.plain_result("❌ 获取赛事信息失败")
            return

        name = data.get("name", "未知")
        brief = data.get("brief", "")
        start_at = data.get("start_at", 0)
        end_at = data.get("end_at", 0)
        start_str = time.strftime("%Y/%m/%d %H:%M", time.localtime(start_at)) if start_at else "未知"
        end_str = time.strftime("%Y/%m/%d %H:%M", time.localtime(end_at)) if end_at else "未知"

        msg = f"""🎯 赛事: {name}
📝 简介: {brief}
🕐 时间: {start_str} - {end_str}
🔗 链接: {self.api_base_url.replace('/api', '')}/games/{self.game_id}"""
        yield event.plain_result(msg)

    @filter.command("rank")
    async def query_rank(self, event: AstrMessageEvent):
        """查询积分排行：/rank 或 /rank [标签]"""
        if not self.game_id:
            yield event.plain_result("❌ 未配置有效的 WebSocket 链接")
            return

        args = event.message_str.strip().split()
        tag = args[1] if len(args) > 1 else None

        if tag:
            # 方向榜
            data = await self._http_get(f"/game/{self.game_id}/challenge/")
            logger.info(f"🔍 方向排行 API 返回: {data}")

            if not data:
                yield event.plain_result("❌ 获取方向排行失败")
                return

            # 解析数据
            if isinstance(data, list) and len(data) > 0:
                challenges = data[0] if isinstance(data[0], list) else data
            else:
                challenges = data if isinstance(data, list) else []

            # 筛选匹配标签的题目
            tag_challenges = []
            for c in challenges:
                if not isinstance(c, dict):
                    continue
                tags = c.get("tag", [])
                if tags:
                    tag_name = tags[0].get("name", "") if isinstance(tags, list) and tags else ""
                    if tag_name.lower() == tag.lower():
                        tag_challenges.append(c)

            if not tag_challenges:
                yield event.plain_result(f"❔ 找不到标签: {tag}")
                return

            # 计算每个队伍的得分
            team_scores = {}
            for c in tag_challenges:
                cid = c.get("id")
                if not cid:
                    continue
                sub_data = await self._http_get(f"/game/{self.game_id}/challenge/{cid}/submission")
                if sub_data:
                    subs = sub_data[0] if isinstance(sub_data, list) and sub_data else []
                    for s in subs:
                        team_name = s.get("team_name")
                        if team_name:
                            team_scores[team_name] = team_scores.get(team_name, 0) + s.get("score", 0)

            sorted_teams = sorted(team_scores.items(), key=lambda x: -x[1])[:10]

            if not sorted_teams:
                yield event.plain_result(f"🏆 [{tag}] 方向暂无得分")
                return

            lines = [f"🏆 [{tag}] 方向排名"]
            medals = ["🥇", "🥈", "🥉"]
            for i, (name, score) in enumerate(sorted_teams):
                medal = medals[i] if i < 3 else f"{i+1}."
                lines.append(f"{medal} {name}: {score} pts")

            yield event.plain_result("\n".join(lines))

        else:
            # 总榜
            data = await self._http_get(f"/game/{self.game_id}/team?page=1&page_size=10&order=score&asc=false&min_state=3")
            if not data:
                yield event.plain_result("❌ 获取总榜失败")
                return

            teams = data[0] if isinstance(data, list) and data else []
            if not teams:
                yield event.plain_result("🏆 暂无队伍得分")
                return

            lines = ["🏆 积分板"]
            medals = ["🥇", "🥈", "🥉"]
            for i, t in enumerate(teams):
                medal = medals[i] if i < 3 else f"{i+1}."
                lines.append(f"{medal} {t.get('name', '未知')}: {t.get('score', 0)} pts")

            yield event.plain_result("\n".join(lines))

    @filter.command("challenge")
    async def query_challenge(self, event: AstrMessageEvent):
        """查询题目详情：/challenge [题目ID]"""
        if not self.game_id:
            yield event.plain_result("❌ 未配置有效的 WebSocket 链接")
            return

        args = event.message_str.strip().split()
        if len(args) < 2:
            yield event.plain_result("❔ 请输入题目 ID，用法: /challenge 123")
            return

        try:
            challenge_id = int(args[1])
        except ValueError:
            yield event.plain_result("❌ 题目 ID 必须是数字")
            return

        data = await self._http_get(f"/game/{self.game_id}/challenge/{challenge_id}")
        if data is None:
            yield event.plain_result("❔ 找不到该题目。")
            return

        if data.get("hidden"):
            yield event.plain_result("🏳️ 该题目未公开。")
            return

        name = data.get("name", "未知")
        tag_name = data.get("tag", [{}])[0].get("name", "未知") if data.get("tag") else "未知"
        score = data.get("score", 0)

        submit_data = await self._http_get(f"/game/{self.game_id}/challenge/{challenge_id}/submit")
        solves = submit_data.get("solves", 0) if submit_data else 0

        msg = f"""🚩 题目: [{name}]
🏷️ 方向: [{tag_name}]
📊 当前分数: {score} pts
👥 已解出数: {solves}"""
        yield event.plain_result(msg)

    @filter.command("team")
    async def query_team(self, event: AstrMessageEvent):
        """查询队伍详情：/team [队伍ID]"""
        if not self.game_id:
            yield event.plain_result("❌ 未配置有效的 WebSocket 链接")
            return

        args = event.message_str.strip().split()
        if len(args) < 2:
            yield event.plain_result("❔ 请输入队伍 ID，用法: /team 123")
            return

        try:
            team_id = int(args[1])
        except ValueError:
            yield event.plain_result("❌ 队伍 ID 必须是数字")
            return

        data = await self._http_get(f"/game/{self.game_id}/team/{team_id}")
        if data is None:
            yield event.plain_result("❔ 找不到该队伍。")
            return

        name = data.get("name", "未知")
        tag = data.get("tag")
        score = data.get("score", 0)
        rank_data = await self._http_get(f"/game/{self.game_id}/team/{team_id}/rank")
        rank = rank_data if rank_data is not None else "未知"

        msg = f"""🧑‍💻 队伍: [{name}]
{'' if tag is None else f'🏷️ 标签: {tag}\n'}📊 当前分数: {score} pts
📈 当前排名: {rank}"""
        yield event.plain_result(msg)

    @filter.command("ret2shell_status")
    async def status(self, event: AstrMessageEvent):
        status_msg = f"""
📊 Ret2Shell 插件状态:
- WebSocket 连接: {'🟢 运行中' if self.running else '🔴 已停止'}
- WS 地址: {self.ws_url[:50] + '...' if self.ws_url else '未配置'}
- Game ID: {self.game_id or '未解析'}
- 公开目标: {self.public_umo or '未配置'}
- 管理目标: {self.admin_umo or '未配置'}
- 运维目标: {self.ops_umo or '未配置'}
- 比赛开始: {self._game_start_time.strftime('%Y-%m-%d %H:%M') if self._game_start_time else '未获取'}
- 比赛结束: {self._game_end_time.strftime('%Y-%m-%d %H:%M') if self._game_end_time else '未获取'}
- 已开启事件: {len(self.enabled_events)} 项
        """.strip()
        yield event.plain_result(status_msg)

    async def terminate(self):
        self.running = False
        if self.ws_task and not self.ws_task.done():
            self.ws_task.cancel()
            try:
                await self.ws_task
            except:
                pass
        if self.client:
            await self.client.aclose()
        logger.info("🔌 Ret2Shell 插件已清理")