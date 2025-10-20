import re
import os
import html
import json
import csv
import argparse
import sys
from datetime import datetime
from collections import Counter, defaultdict
from typing import Dict, List, Tuple, Optional

try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False
    print("Note: Install 'tqdm' for better progress bars: pip install tqdm")

class DiscordExtractor:
    def __init__(self, input_file: str, target_user_ids: List[str], 
                 date_from: Optional[str] = None, date_to: Optional[str] = None,
                 search_term: Optional[str] = None, exclude_replies: bool = False):
        self.input_file = input_file
        self.target_user_ids = target_user_ids
        self.date_from = date_from
        self.date_to = date_to
        self.search_term = search_term.lower() if search_term else None
        self.exclude_replies = exclude_replies
        
        self.group_start_re = re.compile(r'<div\s+class\s*=\s*chatlog__message-group', re.IGNORECASE)
        self.container_re = re.compile(
            r'<div\s+id\s*=\s*chatlog__message-container[^>]*>(.*?)(?=(?:<div\s+id\s*=\s*chatlog__message-container)|$)',
            re.IGNORECASE | re.DOTALL
        )
        self.author_re = re.compile(r'<span\s+class\s*=\s*chatlog__author[^>]*data-user-id\s*=\s*(\d+)[^>]*>(.*?)</span>', re.IGNORECASE | re.DOTALL)
        self.color_re = re.compile(r'style\s*=\s*color\s*:\s*([^ >;"]+)', re.IGNORECASE)
        self.full_ts_re = re.compile(r'<span\s+class\s*=\s*chatlog__timestamp[^>]*>.*?<a[^>]*>(.*?)</a>', re.IGNORECASE | re.DOTALL)
        self.title_ts_re = re.compile(r'<span\s+class\s*=\s*chatlog__timestamp[^>]*title\s*=\s*["\'](.*?)["\']', re.IGNORECASE | re.DOTALL)
        self.short_ts_re = re.compile(r'<div\s+class\s*=\s*chatlog__short-timestamp[^>]*>(.*?)</div>', re.IGNORECASE | re.DOTALL)
        self.inner_span_re = re.compile(r'<span\s+class\s*=\s*chatlog__markdown-preserve[^>]*>(.*?)</span>', re.IGNORECASE | re.DOTALL)
        self.content_div_re = re.compile(r'<div\s+class\s*=\s*(?:"|\')?chatlog__content[^>]*>(.*?)</div>', re.IGNORECASE | re.DOTALL)
        self.attachment_href_re = re.compile(r'<a\s+[^>]*href\s*=\s*["\']([^"\']+)["\'][^>]*>', re.IGNORECASE)
        self.attachment_marker_re = re.compile(r'class\s*=\s*chatlog__attachment', re.IGNORECASE)
        self.img_alt_re = re.compile(r'<img[^>]+alt\s*=\s*["\']([^"\'>]+?)["\']', re.IGNORECASE)
        self.reply_div_re = re.compile(r'<div\s+class\s*=\s*chatlog__reply[^>]*>(.*?)</div>\s*<div\s+class\s*=\s*chatlog__header', re.IGNORECASE | re.DOTALL)
        self.reply_link_re = re.compile(r'scrollToMessage\(event,[\'"](\d+)[\'"]\)', re.IGNORECASE)
        self.message_id_re = re.compile(r'<div\s+id\s*=\s*chatlog__message-container-(\d+)', re.IGNORECASE)
        self.tag_strip_re = re.compile(r'<[^>]+>')
        
        self.all_messages = {}
        self.user_data = {}
        self.statistics = {}
        self.timestamp_formats = [
            "%m/%d/%Y %I:%M %p",
            "%m/%d/%Y %H:%M",
            "%Y-%m-%d %H:%M:%S",
        ]
        
    def strip_tags(self, s: str) -> str:
        return self.tag_strip_re.sub('', s)
    
    def normalize_spaces(self, s: str) -> str:
        return (s or "").replace('\u202f', ' ').replace('\xa0', ' ').replace('\u2009', ' ').strip()
    
    def parse_timestamp(self, ts_str: str) -> Optional[datetime]:
        for fmt in self.timestamp_formats:
            try:
                return datetime.strptime(ts_str, fmt)
            except:
                continue
        return None
    
    def update_progress(self, pbar, amount):
        if HAS_TQDM and pbar:
            pbar.update(amount)
    
    def extract_all_messages(self):
        print("PASS 1: Collecting all messages...")
        
        total_bytes = os.path.getsize(self.input_file)
        groups_seen = 0
        containers_seen = 0
        
        last_full_date = None
        prev_author_id = None
        prev_author_name = None
        prev_author_color = None
        
        group_buf = []
        in_group = False
        
        pbar = tqdm(total=total_bytes, unit='B', unit_scale=True, desc="Reading") if HAS_TQDM else None
        
        def process_container(cont, cont_match):
            nonlocal prev_author_id, prev_author_name, prev_author_color
            nonlocal last_full_date, containers_seen
            
            containers_seen += 1
            
            msg_id_match = self.message_id_re.search(cont_match.group(0))
            current_msg_id = msg_id_match.group(1) if msg_id_match else None
            
            if not current_msg_id:
                return
            
            reply_msg_id = None
            reply_match = self.reply_div_re.search(cont)
            if reply_match:
                reply_section = reply_match.group(1)
                rl = self.reply_link_re.search(reply_section)
                if rl:
                    reply_msg_id = rl.group(1)
            
            a = self.author_re.search(cont)
            if a:
                prev_author_id = a.group(1).strip()
                prev_author_name = self.strip_tags(a.group(2)).strip() if a.group(2) else None
                c = self.color_re.search(cont)
                if c:
                    prev_author_color = c.group(1).strip()
            
            author_id = prev_author_id or ""
            author_name = prev_author_name or "Unknown"
            
            ts = None
            t_full = self.full_ts_re.search(cont)
            t_title = self.title_ts_re.search(cont)
            if t_full:
                ts_raw = self.normalize_spaces(self.strip_tags(t_full.group(1)))
                ts = ts_raw
                parts = ts_raw.split()
                date_part = None
                for p in parts:
                    if '/' in p:
                        date_part = p
                        break
                if date_part:
                    last_full_date = date_part
            elif t_title:
                ts_raw = self.normalize_spaces(self.strip_tags(t_title.group(1)))
                ts = ts_raw
            else:
                short_ts = self.short_ts_re.search(cont)
                if short_ts:
                    st = self.normalize_spaces(self.strip_tags(short_ts.group(1)))
                    if last_full_date:
                        ts = f"{last_full_date} {st}"
                    else:
                        ts = st
            
            content_raw = ""
            m = self.inner_span_re.search(cont)
            if m:
                content_raw = m.group(1)
            else:
                m2 = self.content_div_re.search(cont)
                content_raw = m2.group(1) if m2 else ""
            
            content_text = self.strip_tags(content_raw).strip()
            
            if not content_text:
                if self.attachment_marker_re.search(cont):
                    href = None
                    ah = self.attachment_href_re.search(cont)
                    if ah:
                        href = ah.group(1)
                    if href:
                        filename = href.split('/')[-1].split('?')[0]
                        content_text = f"[Attachment: {filename}]"
                    else:
                        content_text = "[Attachment]"
                else:
                    ia = self.img_alt_re.search(cont)
                    if ia:
                        alt = ia.group(1).strip()
                        content_text = f"[Image/Emoji: {alt}]"
            
            content_text = html.unescape(content_text)
            
            self.all_messages[current_msg_id] = {
                'user_id': author_id,
                'username': author_name,
                'color': prev_author_color,
                'content': content_text,
                'timestamp': ts or "UNKNOWN_TIMESTAMP",
                'reply_to_msg_id': reply_msg_id,
                'message_id': current_msg_id
            }
        
        def process_group(group_text):
            nonlocal groups_seen
            groups_seen += 1
            
            for cont_match in self.container_re.finditer(group_text):
                process_container(cont_match.group(1), cont_match)
        
        with open(self.input_file, "r", encoding="utf-8", errors="ignore") as infile:
            while True:
                line = infile.readline()
                if not line:
                    if in_group and group_buf:
                        process_group(''.join(group_buf))
                    break
                
                self.update_progress(pbar, len(line.encode('utf-8')))
                
                if self.group_start_re.search(line):
                    if in_group and group_buf:
                        process_group(''.join(group_buf))
                        group_buf = []
                    in_group = True
                
                if in_group:
                    group_buf.append(line)
        
        if pbar:
            pbar.close()
        
        print(f"✓ Collected {len(self.all_messages):,} total messages from {groups_seen:,} groups")
    
    def should_include_message(self, msg: Dict) -> bool:
        if self.exclude_replies and msg['reply_to_msg_id']:
            return False
        
        if self.search_term and self.search_term not in msg['content'].lower():
            return False
        
        if self.date_from or self.date_to:
            dt = self.parse_timestamp(msg['timestamp'])
            if dt:
                if self.date_from:
                    from_dt = self.parse_timestamp(self.date_from)
                    if from_dt and dt < from_dt:
                        return False
                if self.date_to:
                    to_dt = self.parse_timestamp(self.date_to)
                    if to_dt and dt > to_dt:
                        return False
        
        return True
    
    def filter_and_extract_users(self):
        print("\nPASS 2: Filtering and extracting target user messages...")
        
        for user_id in self.target_user_ids:
            messages = []
            replied_to_users = {}
            first_timestamp = None
            last_timestamp = None
            username = None
            color = None
            
            for msg_id, msg in self.all_messages.items():
                if msg['user_id'] != user_id:
                    continue
                
                if not self.should_include_message(msg):
                    continue
                
                if not username:
                    username = msg['username']
                    color = msg['color']
                    first_timestamp = msg['timestamp']
                
                last_timestamp = msg['timestamp']
                
                reply_chain_ids = []
                if msg['reply_to_msg_id'] and msg['reply_to_msg_id'] in self.all_messages:
                    replied_msg = self.all_messages[msg['reply_to_msg_id']]
                    reply_user_id = replied_msg['user_id']
                    
                    if reply_user_id != user_id:
                        if reply_user_id not in replied_to_users:
                            replied_to_users[reply_user_id] = (replied_msg['username'], 0)
                        replied_to_users[reply_user_id] = (replied_msg['username'], replied_to_users[reply_user_id][1] + 1)
                    
                    reply_chain_ids = self.build_reply_chain_ids(msg['reply_to_msg_id'])
                
                messages.append({
                    'timestamp': msg['timestamp'],
                    'content': msg['content'],
                    'reply_to_msg_id': msg['reply_to_msg_id'],
                    'reply_chain_ids': reply_chain_ids,
                    'message_id': msg_id
                })
            
            self.user_data[user_id] = {
                'username': username or 'Unknown',
                'color': color or 'N/A',
                'messages': messages,
                'replied_to_users': replied_to_users,
                'first_timestamp': first_timestamp,
                'last_timestamp': last_timestamp
            }
            
            print(f"✓ User {username} ({user_id}): {len(messages):,} messages")
    
    def build_reply_chain_ids(self, msg_id: str, max_depth: int = 5) -> List[str]:
        chain = []
        current_id = msg_id
        depth = 0
        
        while current_id and current_id in self.all_messages and depth < max_depth:
            chain.append(current_id)
            current_id = self.all_messages[current_id]['reply_to_msg_id']
            depth += 1
        
        return chain
    
    def calculate_statistics(self):
        print("\nPASS 3: Calculating statistics...")
        
        for user_id, data in self.user_data.items():
            messages = data['messages']
            
            if not messages:
                continue
            
            total_messages = len(messages)
            reply_count = sum(1 for m in messages if m['reply_to_msg_id'])
            original_count = total_messages - reply_count
            
            all_text = ' '.join(
                self.all_messages[m['message_id']]['content'] 
                for m in messages 
                if not self.all_messages[m['message_id']]['content'].startswith('[')
            )
            words = all_text.split()
            total_words = len(words)
            avg_length = total_words / total_messages if total_messages > 0 else 0

            timestamps_parsed = []
            for m in messages:
                dt = self.parse_timestamp(m['timestamp'])
                if dt:
                    timestamps_parsed.append(dt)
            
            hour_distribution = Counter(dt.hour for dt in timestamps_parsed)
            day_distribution = Counter(dt.strftime('%A') for dt in timestamps_parsed)
            
            reply_depth = [len(m['reply_chain_ids']) for m in messages if m['reply_chain_ids']]
            avg_reply_depth = sum(reply_depth) / len(reply_depth) if reply_depth else 0
            
            self.statistics[user_id] = {
                'total_messages': total_messages,
                'original_messages': original_count,
                'replies': reply_count,
                'total_words': total_words,
                'avg_message_length': avg_length,
                'most_active_hour': hour_distribution.most_common(1)[0] if hour_distribution else None,
                'most_active_day': day_distribution.most_common(1)[0] if day_distribution else None,
                'avg_reply_depth': avg_reply_depth,
                'hour_distribution': dict(hour_distribution),
                'day_distribution': dict(day_distribution)
            }
    
    def get_output_path(self, prefix: str, username: str, user_id: str, ext: str) -> str:
        return f"{prefix}_{username}_{user_id}.{ext}"
    
    def format_date_range(self, first_ts: str, last_ts: str) -> str:
        if first_ts and last_ts:
            return f"{first_ts} → {last_ts}"
        return "N/A"
    
    def get_reply_message(self, msg_id: str) -> Optional[Dict]:
        return self.all_messages.get(msg_id)
    
    def export_txt(self, output_file: str, user_id: str):
        data = self.user_data[user_id]
        stats = self.statistics.get(user_id, {})
        
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write("=" * 60 + "\n")
            f.write("Discord Message Archive\n")
            f.write("=" * 60 + "\n")
            f.write(f"User ID   : {user_id}\n")
            f.write(f"Username  : {data['username']}\n")
            f.write(f"Color     : {data['color']}\n")
            f.write(f"Messages  : {len(data['messages']):,}\n")
            date_range = self.format_date_range(data['first_timestamp'], data['last_timestamp'])
            if date_range != "N/A":
                f.write(f"Range     : {date_range}\n")
            
            if data['replied_to_users']:
                f.write("\n" + "-" * 60 + "\n")
                f.write("Reply Summary\n")
                f.write("-" * 60 + "\n")
                f.write(f"Replied to {len(data['replied_to_users'])} unique user(s):\n")
                sorted_replies = sorted(data['replied_to_users'].items(), key=lambda x: x[1][1], reverse=True)
                for uid, (uname, count) in sorted_replies[:10]:
                    f.write(f"  • {uname} (ID: {uid}): {count} time{'s' if count != 1 else ''}\n")
            
            if stats:
                f.write("\n" + "-" * 60 + "\n")
                f.write("Statistics\n")
                f.write("-" * 60 + "\n")
                f.write(f"Total Words       : {stats['total_words']:,}\n")
                f.write(f"Avg Message Length: {stats['avg_message_length']:.1f} words\n")
                f.write(f"Original Messages : {stats['original_messages']:,}\n")
                f.write(f"Replies           : {stats['replies']:,}\n")
                
                if stats['most_active_hour']:
                    hour, count = stats['most_active_hour']
                    f.write(f"Most Active Hour  : {hour}:00 ({count} messages)\n")
                
                if stats['most_active_day']:
                    day, count = stats['most_active_day']
                    f.write(f"Most Active Day   : {day} ({count} messages)\n")
            
            f.write("\n" + "=" * 60 + "\n\n")
            
            for msg in data['messages']:
                if msg['reply_chain_ids'] and len(msg['reply_chain_ids']) > 1:
                    f.write("┌─ [CONTEXT CHAIN] " + "─" * 38 + "\n")
                    for i, chain_msg_id in enumerate(reversed(msg['reply_chain_ids'])):
                        chain_msg = self.all_messages[chain_msg_id]
                        indent = "│ " + "  " * i
                        f.write(f"{indent}[{chain_msg['timestamp']}] {chain_msg['username']} (ID: {chain_msg['user_id']}):\n")
                        f.write(f"{indent}{chain_msg['content']}\n")
                        if i < len(msg['reply_chain_ids']) - 1:
                            f.write(f"{indent}↳\n")
                    f.write("└" + "─" * 59 + "\n")
                elif msg['reply_to_msg_id']:
                    replied = self.get_reply_message(msg['reply_to_msg_id'])
                    if replied:
                        f.write("┌─ [CONTEXT] " + "─" * 47 + "\n")
                        f.write(f"│ [{replied['timestamp']}] {replied['username']} (ID: {replied['user_id']}):\n")
                        f.write(f"│ {replied['content']}\n")
                        f.write("└" + "─" * 59 + "\n")
                
                f.write(f"[{msg['timestamp']}] {user_id}: {self.all_messages[msg['message_id']]['content']}\n\n")
    
    def export_json(self, output_file: str, user_id: str):
        data = self.user_data[user_id]
        stats = self.statistics.get(user_id, {})
        
        output = {
            'user_id': user_id,
            'username': data['username'],
            'color': data['color'],
            'message_count': len(data['messages']),
            'date_range': {
                'first': data['first_timestamp'],
                'last': data['last_timestamp']
            },
            'statistics': stats,
            'replied_to_users': [
                {'user_id': uid, 'username': uname, 'count': count}
                for uid, (uname, count) in data['replied_to_users'].items()
            ],
            'messages': []
        }
        
        for msg in data['messages']:
            msg_data = {
                'timestamp': msg['timestamp'],
                'content': self.all_messages[msg['message_id']]['content'],
                'message_id': msg['message_id'],
                'reply_chain_length': len(msg['reply_chain_ids'])
            }
            
            if msg['reply_to_msg_id']:
                replied = self.get_reply_message(msg['reply_to_msg_id'])
                if replied:
                    msg_data['reply_to'] = {
                        'user_id': replied['user_id'],
                        'username': replied['username'],
                        'content': replied['content'],
                        'timestamp': replied['timestamp']
                    }
                else:
                    msg_data['reply_to'] = None
            else:
                msg_data['reply_to'] = None
            
            output['messages'].append(msg_data)
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
    
    def export_csv(self, output_file: str, user_id: str):
        data = self.user_data[user_id]
        
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['Timestamp', 'User ID', 'Username', 'Content', 'Reply To User', 'Reply To Content', 'Message ID'])
            
            for msg in data['messages']:
                replied = self.get_reply_message(msg['reply_to_msg_id']) if msg['reply_to_msg_id'] else None
                writer.writerow([
                    msg['timestamp'],
                    user_id,
                    data['username'],
                    self.all_messages[msg['message_id']]['content'],
                    replied['username'] if replied else '',
                    replied['content'] if replied else '',
                    msg['message_id']
                ])
    
    def export_markdown(self, output_file: str, user_id: str):
        data = self.user_data[user_id]
        stats = self.statistics.get(user_id, {})
        
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(f"# Discord Message Archive\n\n")
            f.write(f"## User Information\n\n")
            f.write(f"- **User ID**: {user_id}\n")
            f.write(f"- **Username**: {data['username']}\n")
            f.write(f"- **Messages**: {len(data['messages']):,}\n")
            date_range = self.format_date_range(data['first_timestamp'], data['last_timestamp'])
            if date_range != "N/A":
                f.write(f"- **Date Range**: {date_range}\n")
            
            if data['replied_to_users']:
                f.write(f"\n## Reply Summary\n\n")
                f.write(f"Replied to {len(data['replied_to_users'])} unique user(s):\n\n")
                sorted_replies = sorted(data['replied_to_users'].items(), key=lambda x: x[1][1], reverse=True)
                for uid, (uname, count) in sorted_replies[:10]:
                    f.write(f"- **{uname}** (ID: {uid}): {count} time{'s' if count != 1 else ''}\n")
            
            if stats:
                f.write(f"\n## Statistics\n\n")
                f.write(f"| Metric | Value |\n")
                f.write(f"|--------|-------|\n")
                f.write(f"| Total Words | {stats['total_words']:,} |\n")
                f.write(f"| Avg Message Length | {stats['avg_message_length']:.1f} words |\n")
                f.write(f"| Original Messages | {stats['original_messages']:,} |\n")
                f.write(f"| Replies | {stats['replies']:,} |\n")
                
                if stats['most_active_hour']:
                    hour, count = stats['most_active_hour']
                    f.write(f"| Most Active Hour | {hour}:00 ({count} messages) |\n")
                
                if stats['most_active_day']:
                    day, count = stats['most_active_day']
                    f.write(f"| Most Active Day | {day} ({count} messages) |\n")
            
            f.write(f"\n## Messages\n\n")
            
            for msg in data['messages']:
                if msg['reply_to_msg_id']:
                    replied = self.get_reply_message(msg['reply_to_msg_id'])
                    if replied:
                        f.write(f"> **{replied['username']}** ({replied['timestamp']}):  \n")
                        f.write(f"> {replied['content']}\n\n")
                
                f.write(f"**{data['username']}** ({msg['timestamp']}):  \n")
                f.write(f"{self.all_messages[msg['message_id']]['content']}\n\n")
                f.write("---\n\n")
    
    def export_html(self, output_file: str, user_id: str):
        data = self.user_data[user_id]
        stats = self.statistics.get(user_id, {})
        
        html_template = """
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>Discord Archive - {username}</title>
    <style>
        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background-color: #36393f;
            color: #dcddde;
            padding: 20px;
            max-width: 1200px;
            margin: 0 auto;
        }}
        .header {{
            background-color: #2f3136;
            padding: 20px;
            border-radius: 8px;
            margin-bottom: 20px;
        }}
        .stats {{
            background-color: #2f3136;
            padding: 20px;
            border-radius: 8px;
            margin-bottom: 20px;
        }}
        .message {{
            background-color: #2f3136;
            padding: 15px;
            border-radius: 8px;
            margin-bottom: 10px;
            border-left: 3px solid {user_color};
        }}
        .context {{
            background-color: #202225;
            padding: 10px;
            border-radius: 4px;
            margin-bottom: 10px;
            border-left: 2px solid #7289da;
        }}
        .timestamp {{
            color: #72767d;
            font-size: 0.9em;
        }}
        .username {{
            color: {user_color};
            font-weight: bold;
        }}
        h1, h2 {{
            color: #fff;
        }}
        .stat-item {{
            margin: 10px 0;
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1>Discord Message Archive</h1>
        <p><strong>User ID:</strong> {user_id}</p>
        <p><strong>Username:</strong> {username}</p>
        <p><strong>Messages:</strong> {message_count}</p>
        <p><strong>Date Range:</strong> {date_range}</p>
    </div>
    
    {stats_html}
    
    <h2>Messages</h2>
    {messages_html}
</body>
</html>
"""
        
        stats_html = ""
        if stats:
            stats_html = '<div class="stats"><h2>Statistics</h2>'
            stats_html += f'<div class="stat-item"><strong>Total Words:</strong> {stats["total_words"]:,}</div>'
            stats_html += f'<div class="stat-item"><strong>Avg Message Length:</strong> {stats["avg_message_length"]:.1f} words</div>'
            stats_html += f'<div class="stat-item"><strong>Original Messages:</strong> {stats["original_messages"]:,}</div>'
            stats_html += f'<div class="stat-item"><strong>Replies:</strong> {stats["replies"]:,}</div>'
            if stats['most_active_hour']:
                hour, count = stats['most_active_hour']
                stats_html += f'<div class="stat-item"><strong>Most Active Hour:</strong> {hour}:00 ({count} messages)</div>'
            if stats['most_active_day']:
                day, count = stats['most_active_day']
                stats_html += f'<div class="stat-item"><strong>Most Active Day:</strong> {day} ({count} messages)</div>'
            stats_html += '</div>'
        
        messages_html = ""
        for msg in data['messages']:
            if msg['reply_to_msg_id']:
                replied = self.get_reply_message(msg['reply_to_msg_id'])
                if replied:
                    messages_html += f'<div class="context">'
                    messages_html += f'<span class="username">{html.escape(replied["username"])}</span> '
                    messages_html += f'<span class="timestamp">{html.escape(replied["timestamp"])}</span><br>'
                    messages_html += f'{html.escape(replied["content"])}'
                    messages_html += '</div>'
            
            messages_html += '<div class="message">'
            messages_html += f'<span class="username">{html.escape(data["username"])}</span> '
            messages_html += f'<span class="timestamp">{html.escape(msg["timestamp"])}</span><br>'
            messages_html += f'{html.escape(self.all_messages[msg["message_id"]]["content"])}'
            messages_html += '</div>'
        
        html_output = html_template.format(
            username=html.escape(data['username']),
            user_id=html.escape(user_id),
            user_color=data['color'] or '#7289da',
            message_count=f"{len(data['messages']):,}",
            date_range=self.format_date_range(data['first_timestamp'], data['last_timestamp']),
            stats_html=stats_html,
            messages_html=messages_html
        )
        
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(html_output)
    
    def run(self, output_formats: List[str], output_prefix: str):
        self.extract_all_messages()
        self.filter_and_extract_users()
        self.calculate_statistics()
        
        print(f"\nExporting results...")
        for user_id in self.target_user_ids:
            if user_id not in self.user_data:
                print(f"⚠ No data found for user ID {user_id}")
                continue
            
            username = self.user_data[user_id]['username']
            
            export_methods = {
                'txt': self.export_txt,
                'json': self.export_json,
                'csv': self.export_csv,
                'md': self.export_markdown,
                'html': self.export_html
            }
            
            for fmt in output_formats:
                if fmt in export_methods:
                    output_file = self.get_output_path(output_prefix, username, user_id, fmt)
                    export_methods[fmt](output_file, user_id)
                    print(f"✓ Exported {fmt.upper()}: {output_file}")
        
        print("\n✅ All exports complete!")


def main():
    parser = argparse.ArgumentParser(
        description='Discord Message Extractor Pro - Extract and analyze Discord chat logs',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic extraction (single user)
  %(prog)s --input chat.html --user-id 123456789
  
  # Multiple users with JSON output
  %(prog)s --input chat.html --user-ids 123456789,987654321 --format json
  
  # Filter by date range
  %(prog)s --input chat.html --user-id 123456789 --date-from "10/01/2023" --date-to "10/31/2023"
  
  # Search for specific term
  %(prog)s --input chat.html --user-id 123456789 --search "gaming"
  
  # Export all formats
  %(prog)s --input chat.html --user-id 123456789 --format txt,json,csv,md,html
  
  # Exclude replies
  %(prog)s --input chat.html --user-id 123456789 --exclude-replies
        """
    )
    
    parser.add_argument('-i', '--input', required=True, help='Input HTML file from Discord chat export')
    parser.add_argument('-o', '--output', default='discord_export', help='Output file prefix (default: discord_export)')
    parser.add_argument('-u', '--user-id', dest='user_id', help='Single target user ID to extract')
    parser.add_argument('-U', '--user-ids', dest='user_ids', help='Multiple user IDs (comma-separated)')
    parser.add_argument('-f', '--format', default='txt', help='Output format(s): txt,json,csv,md,html (comma-separated, default: txt)')
    parser.add_argument('--date-from', help='Filter messages from this date (format: MM/DD/YYYY)')
    parser.add_argument('--date-to', help='Filter messages until this date (format: MM/DD/YYYY)')
    parser.add_argument('-s', '--search', help='Search for messages containing this term')
    parser.add_argument('--exclude-replies', action='store_true', help='Exclude reply messages')
    
    args = parser.parse_args()
    
    if not os.path.exists(args.input):
        print(f"❌ Error: Input file '{args.input}' not found")
        sys.exit(1)
    
    user_ids = []
    if args.user_id:
        user_ids.append(args.user_id)
    if args.user_ids:
        user_ids.extend(args.user_ids.split(','))
    
    if not user_ids:
        print("❌ Error: At least one user ID must be specified (--user-id or --user-ids)")
        sys.exit(1)
    
    user_ids = [uid.strip() for uid in user_ids]
    
    formats = [f.strip().lower() for f in args.format.split(',')]
    valid_formats = {'txt', 'json', 'csv', 'md', 'html'}
    invalid = set(formats) - valid_formats
    if invalid:
        print(f"❌ Error: Invalid format(s): {', '.join(invalid)}")
        print(f"Valid formats: {', '.join(valid_formats)}")
        sys.exit(1)
    
    print("=" * 60)
    print("Discord Message Extractor Pro")
    print("=" * 60)
    print(f"Input file: {args.input}")
    print(f"Target user IDs: {', '.join(user_ids)}")
    print(f"Output formats: {', '.join(formats)}")
    if args.date_from:
        print(f"Date from: {args.date_from}")
    if args.date_to:
        print(f"Date to: {args.date_to}")
    if args.search:
        print(f"Search term: {args.search}")
    if args.exclude_replies:
        print("Excluding reply messages")
    print("=" * 60 + "\n")
    
    extractor = DiscordExtractor(
        input_file=args.input,
        target_user_ids=user_ids,
        date_from=args.date_from,
        date_to=args.date_to,
        search_term=args.search,
        exclude_replies=args.exclude_replies
    )
    
    extractor.run(formats, args.output)


if __name__ == "__main__":
    main()