# chatter.py
import socket
import threading
import json
import sys
import time
import random
import os
from typing import Dict, List, Tuple, Set, Any


class Chatter:
    """
    Implémente un nœud peer-to-peer pour une application de chat distribué
    tolérante aux pannes, avec ordonnancement causal pour le broadcast et
    FIFO pour les messages privés.
    """

    def __init__(self, node_id: int, all_nodes: Dict[int, Tuple[str, int]]):
        self.node_id = node_id
        self.host, self.port = all_nodes[node_id]
        self.peers = {nid: addr for nid, addr in all_nodes.items() if nid != self.node_id}

        self.session_id = random.randint(1000, 9999)
        print(f"[{self.node_id}] Session ID: {self.session_id}")

        # --- CORRECTION : Mémoriser les sessions des pairs ---
        # Initialisé à None, on apprendra le session_id au premier message reçu.
        self.peer_sessions: Dict[int, int] = {peer_id: None for peer_id in all_nodes}

        self.vector_clock = [0] * len(all_nodes)
        self.message_buffer: Set[str] = set()
        self.private_message_buffer: Set[str] = set()
        self.sent_seq_numbers: Dict[int, int] = {peer_id: 0 for peer_id in self.peers}
        self.received_seq_numbers: Dict[int, int] = {peer_id: 0 for peer_id in self.peers}
        self.seen_messages: Set[str] = set()
        self.lock = threading.Lock()
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((self.host, self.port))

    def _listen(self):
        print(f"[{self.node_id}] Écoute sur {self.host}:{self.port}")
        while True:
            try:
                data, _ = self.sock.recvfrom(4096)
                self._handle_message(data)
            except OSError:
                break
            except Exception as e:
                print(f"[{self.node_id}] Erreur de réception: {e}")

    def _handle_message(self, data: bytes):
        time.sleep(random.uniform(0, 0.5))
        try:
            message = json.loads(data.decode())
            msg_id = message["id"]
        except (json.JSONDecodeError, KeyError):
            return

        with self.lock:
            if msg_id in self.seen_messages: return
            self.seen_messages.add(msg_id)

            if message['type'] == 'broadcast':
                relay_message = message.copy()
                relay_message["relayed_by"] = self.node_id
                relay_data = json.dumps(relay_message).encode()
                sender_id = message["sender_id"]
                for peer_id, peer_addr in self.peers.items():
                    if peer_id != sender_id:
                        self.sock.sendto(relay_data, peer_addr)

            if message['type'] == 'broadcast':
                self.message_buffer.add(json.dumps(message))
            elif message['type'] == 'private':
                if message['target_id'] == self.node_id:
                    self.private_message_buffer.add(json.dumps(message))

    def _check_buffers(self):
        while True:
            time.sleep(0.1)
            with self.lock:
                self._process_broadcast_buffer()
                self._process_private_buffer()

    def _process_broadcast_buffer(self):
        # Boucle pour s'assurer que le buffer est traité jusqu'à ce qu'il soit stable
        # (c'est-à-dire qu'aucune nouvelle action, purge ou livraison, n'est possible).
        while True:
            restarted_peer_handled = False

            # --- 1. DÉTECTION ET GESTION DES REDÉMARRAGES ---
            # Itère sur une copie du buffer pour détecter un changement de session.
            for msg_str in list(self.message_buffer):
                msg = json.loads(msg_str)
                sender_id = msg["sender_id"]
                msg_session_id = int(msg["id"].split(':')[1])

                # Apprend le session_id si c'est le premier message de ce pair
                if self.peer_sessions.get(sender_id) is None:
                    self.peer_sessions[sender_id] = msg_session_id

                # Si la session du message est différente de celle que nous connaissons
                elif self.peer_sessions[sender_id] != msg_session_id:
                    old_session_id = self.peer_sessions[sender_id]
                    print(
                        f"\n[INFO] Détection du redémarrage du Nœud {sender_id} (ancienne session: {old_session_id}, nouvelle: {msg_session_id}).")
                    print(f"       -> Purge des messages de la session {old_session_id}.")

                    # Crée un nouveau buffer ne contenant pas les messages de l'ancienne session du pair redémarré.
                    self.message_buffer = {
                        m_str for m_str in self.message_buffer
                        if not (json.loads(m_str)['sender_id'] == sender_id and int(
                            json.loads(m_str)['id'].split(':')[1]) == old_session_id)
                    }

                    # Met à jour l'état du pair avec la nouvelle session et réinitialise son horloge.
                    self.peer_sessions[sender_id] = msg_session_id
                    self.vector_clock[sender_id] = 0

                    restarted_peer_handled = True
                    # Un redémarrage a été traité. Il faut recommencer l'analyse du buffer nettoyé.
                    break

            if restarted_peer_handled:
                # `continue` force la reprise de la `while True` pour garantir que le buffer est analysé dans un état propre.
                continue

            # --- 2. LIVRAISON DES MESSAGES EN ORDRE CAUSAL ---
            # Cette partie ne s'exécute que si aucun redémarrage n'a été traité dans cette itération.
            messages_to_deliver = []
            for msg_str in self.message_buffer:
                msg = json.loads(msg_str)
                sender_id = msg["sender_id"]
                msg_clock = msg["clock"]

                is_next_from_sender = (msg_clock[sender_id] == self.vector_clock[sender_id] + 1)
                others_are_met = all(
                    msg_clock[i] <= self.vector_clock[i] for i in range(len(self.vector_clock)) if i != sender_id
                )
                if is_next_from_sender and others_are_met:
                    messages_to_deliver.append(msg)

            if not messages_to_deliver:
                # Si aucun message n'est prêt à être livré, le buffer est stable. On sort de la boucle.
                break

            # Trie et livre les messages prêts
            messages_to_deliver.sort(key=lambda m: m["clock"])
            for msg in messages_to_deliver:
                relayed_by_info = f"(relayed by {msg['relayed_by']})" if 'relayed_by' in msg and msg[
                    'relayed_by'] != self.node_id else ""
                print(f"\n[Broadcast] <User {msg['sender_id']}>: {msg['text']} {relayed_by_info}")

                # Met à jour l'horloge et retire le message livré du buffer.
                self.vector_clock[msg['sender_id']] += 1
                self.message_buffer.remove(json.dumps(msg))

    def _process_private_buffer(self):
        private_to_deliver = []
        for msg_str in self.private_message_buffer:
            msg = json.loads(msg_str)
            sender_id = msg["sender_id"]
            if msg['seq'] == self.received_seq_numbers.get(sender_id, 0) + 1:
                private_to_deliver.append(msg)
        if private_to_deliver:
            private_to_deliver.sort(key=lambda m: m["seq"])
            for msg in private_to_deliver:
                print(f"\n[Private from {msg['sender_id']}]> {msg['text']}")
                self.received_seq_numbers[msg['sender_id']] = msg['seq']
                self.private_message_buffer.remove(json.dumps(msg))

    def broadcast(self, text: str, crash_after_send: bool = False, fail_target_id: int = None):
        with self.lock:
            self.vector_clock[self.node_id] += 1
            msg_id = f"{self.node_id}:{self.session_id}:{self.vector_clock[self.node_id]}"
            message = {"id": msg_id, "type": "broadcast", "sender_id": self.node_id, "clock": self.vector_clock.copy(),
                       "text": text}
            self.seen_messages.add(msg_id)
            data = json.dumps(message).encode()

            for peer_id, peer_addr in self.peers.items():
                if peer_id == fail_target_id:
                    print(f"[{self.node_id}] ! Panne réseau simulée: N'envoie PAS à {peer_id}")
                    continue
                self.sock.sendto(data, peer_addr)

            if not crash_after_send: print(f"[Broadcast Sent]")

        if crash_after_send:
            time.sleep(5)
            print(f"[{self.node_id}] PANNE SIMULÉE...")
            os._exit(1)

    def send_private(self, target_id: int, text: str):
        if target_id not in self.peers:
            print(f"Erreur : Nœud {target_id} inconnu.")
            return
        with self.lock:
            self.vector_clock[self.node_id] += 1
            self.sent_seq_numbers[target_id] += 1
            seq_num = self.sent_seq_numbers[target_id]
            msg_id = f"{self.node_id}:{self.session_id}:{self.vector_clock[self.node_id]}"
            message = {"id": msg_id, "type": "private", "sender_id": self.node_id, "target_id": target_id,
                       "seq": seq_num, "text": text}
            data = json.dumps(message).encode()
            self.sock.sendto(data, self.peers[target_id])
            print(f"Message privé envoyé à {target_id} (Seq: {seq_num})")

    def run(self):
        listener_thread = threading.Thread(target=self._listen, daemon=True)
        listener_thread.start()
        buffer_checker_thread = threading.Thread(target=self._check_buffers, daemon=True)
        buffer_checker_thread.start()
        print("--- Chat Distribué Démarré ---")
        print(f"Mon ID: {self.node_id}. Pairs connus: {list(self.peers.keys())}")
        print("Commandes:\n  'b <msg>' 'p <id> <msg>' 'crashb <msg>' 'netfailb <id> <msg>' 'quit'")

        while True:
            try:
                user_input = input(f"[{self.node_id}]> ")
                if user_input.lower() == 'quit': break
                parts = user_input.split(" ", 1)
                command = parts[0].lower()

                if command == 'b' and len(parts) > 1:
                    self.broadcast(parts[1])
                elif command == 'crashb' and len(parts) > 1:
                    self.broadcast(parts[1], crash_after_send=True)
                elif command == 'netfailb' and len(parts) > 1:
                    sub_parts = parts[1].split(" ", 1)
                    if len(sub_parts) > 1:
                        try:
                            target_id = int(sub_parts[0])
                            self.broadcast(sub_parts[1], fail_target_id=target_id)
                        except (ValueError, IndexError):
                            print("Format: netfailb <id> <msg>")
                    else:
                        print("Format: netfailb <id> <msg>")
                elif command == 'p' and len(parts) > 1:
                    sub_parts = parts[1].split(" ", 1)
                    if len(sub_parts) > 1:
                        try:
                            target_id = int(sub_parts[0])
                            self.send_private(target_id, sub_parts[1])
                        except (ValueError, IndexError):
                            print("Format: p <id> <msg>")
                    else:
                        print("Format: p <id> <msg>")
                elif command == 'debug':
                    with self.lock:
                        print("\n--- DEBUG INFO ---")
                        print(f"My Vector Clock: {self.vector_clock}")
                        print(f"Broadcast Buffer ({len(self.message_buffer)} items):")
                        for msg_str in sorted(list(self.message_buffer)):
                            msg = json.loads(msg_str)
                            print(
                                f"  - ID: {msg['id']}, From: {msg['sender_id']}, Clock: {msg['clock']}, Text: {msg['text']}")
                        print("------------------\n")
                else:
                    if user_input: print("Commande inconnue.")
            except KeyboardInterrupt:
                break
        self.sock.close()
        print(f"[{self.node_id}] Au revoir!")


if __name__ == "__main__":
    CHAT_GROUP = {i: ("127.0.0.1", 5000 + i) for i in range(10)}

    if len(sys.argv) != 2:
        print(f"Usage: python {sys.argv[0]} <node_id>")
        print(f"Node IDs possibles: {list(CHAT_GROUP.keys())}")
        sys.exit(1)
    try:
        node_id = int(sys.argv[1])
        if node_id not in CHAT_GROUP: raise ValueError
    except ValueError:
        print(f"ID de nœud invalide."); sys.exit(1)
    chatter = Chatter(node_id, CHAT_GROUP)
    chatter.run()