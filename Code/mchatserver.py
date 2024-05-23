# mchatserver.py - Multi-threaded chat server program
# Author: Muhammad Sulaman Khan
# Student Number : 47511921


# Imports
import socket
import threading
import sys
import time
import os
import queue


# Constants
MIN_PORT = 1
MAX_PORT = 49151
MAX_CHANNEL_CAPACITY = 5
CLIENT_INTIAL_TIME = 10
INITIAL_MUTE = 0
WELCOME_MESSAGE = "[Server message (%s)] Welcome to the %s channel, %s."
WELCOME_MESSAGE_QUEUE = "[Server message (%s)] Welcome to the %s waiting" \
                        " room, %s."
QUIT = "[Server message (%s)] %s has left the channel."
QUEUE_UPDATE = "[Server message (%s)] You are in the waiting queue and there" \
               " are %d user(s) ahead of you."
MUTED_SERVER = "[Server message (%s)] Muted %s for %d seconds."
MUTED_CLIENT = "[Server message (%s)] You have been muted for %d seconds."
MUTED_CLIENTS = "[Server message (%s)] %s has been muted for %d seconds."
STILL_MUTED = "[Server message (%s)] You are still muted for %d seconds."
NOT_HERE = "[Server message (%s)] %s is not here."
INVALID_MUTE_TIME = "[Server message (%s)] Invalid mute time."
SENT_CLIENT = "[Server message (%s)] You sent %s to %s."
SENT_SERVER = "[Server message (%s)] %s sent %s to %s."
INVALID_FILE = "[Server message (%s)] %s does not exist."
CHANNEL = "[Channel] %s %d Capacity: %d/ %d, Queue: %d."
CLIENT_MESSAGE = "[%s (%s)] %s"
CLIENT_HANDLER_ERROR = "Error in client handler: %s."
WHISPER = "[%s whispers to you: (%s)] %s"
WHISPER_SERVER = "[%s whispers to %s: (%s)] %s"
DUPLICATE_USER = "[Server message (%s)] %s already has a user with" \
                 " username %s."
JOINED_CLIENT = "[Server message (%s)] %s has joined the channel."
JOINED_SERVER = "[Server message (%s)] %s has joined the %s channel."
INVALID_CHANNEL = "[Server message (%s)] %s does not exist."
CHANNEL_EMPTIED = "[Server message (%s)] %s has been emptied."
INVALID_MUTE_TIME = "[Server message (%s)] Invalid mute time."
AFK = "[Server message (%s)] %s went AFK."
SEND_ERROR = "[Server message (%s)] Usage /send <target> <file_path>."
WHISPER_ERROR = "[Server message (%s)] Usage /whisper <target> <message>."
SWITCH_ERROR = "[Server message (%s)] Usage /switch <channel_name>."
KICK_ERROR = "[Server message (%s)] Usage /kick <channel_name> <username>."
KICKED = "[Server message (%s)] Kicked %s."
EMPTY_ERROR = "[Server message (%s)] Usage /empty <channel_name>."
MUTE_ERROR = "[Server message (%s)] Usage /mute <channel_name> <username> " \
             "<time>."
SPACE = " "
SEND_FILE = "FILE: %s:"
SWITCH_REQUEST = "SWITCH: %s"
QUIT_REQUEST = "QUIT:"


class Client:
    def __init__(self, username, connection, address):
        self.username = username
        self.connection = connection
        self.address = address
        self.kicked = False
        self.in_queue = True
        self.remaining_time = CLIENT_INTIAL_TIME
        self.muted = False
        self.mute_duration = INITIAL_MUTE


class Channel:
    def __init__(self, name, port, capacity):
        self.name = name
        self.port = port
        self.capacity = capacity
        self.queue = queue.Queue()
        self.clients = []


def parse_config(config_file: str) -> list:
    """
    Parses lines from a given configuration file and VALIDATE the format of
    each line. The function validates each part and if valid returns a list of
    tuples where each tuple contains (channel_name, channel_port,
    channel_capacity). The function also ensures that there are no duplicate
    channel names or ports. If not valid, exit with status code 1.
    Status: TODO
    Args:
        config_file (str): The path to the configuration file (e.g, cnfig.txt).
    Returns:
        list: A list of tuples where each tuple contains:
        (channel_name, channel_port, and channel_capacity)
    Raises:
        SystemExit: If there is an error in the configuration file format.
    """
    config_file = open(config_file, "r")
    lines = config_file.readlines()
    config_file.close()

    channels = []
    names = []
    ports = []

    for line in lines:
        channel = line.strip().split()

        if len(channel) != 4:
            sys.exit(1)

        name = channel[1]
        port = int(channel[2])
        capacity = int(channel[3])

        if name.isalpha() and port in range(MIN_PORT, MAX_PORT) \
                and capacity <= MAX_CHANNEL_CAPACITY and name not in names \
                and port not in ports:
            channels.append((name, port, capacity))
            names.append(name)
            ports.append(port)

        else:
            sys.exit(1)

    if len(channels) == 2:
        sys.exit(1)

    return channels


def get_channels_dictionary(parsed_lines) -> dict:
    """
    Creates a dictionary of Channel objects from parsed lines.
    Status: Given
    Args:
        parsed_lines (list): A list of tuples where each tuple contains:
        (name, port, and capacity)
    Returns:
        dict: A dictionary of Channel objects with key as the channel name.
    """
    channels = {}

    for name, port, capacity in parsed_lines:
        channels[name] = Channel(name, port, capacity)

    return channels


def quit_client(client, channel, silent=False, server_silent=False) -> None:
    """
    Implement client quitting function. Used differently in some functions,
    hence the addition of the close and silent parameters, with default values.
    Close is responsible for closing the connection, and silent is responsible
    for not broadcasting the quit message to all clients.
    Status: TODO
    """
    client.connection.sendall(QUIT_REQUEST.encode())

    if client.in_queue:
        channel.queue = remove_item(channel.queue, client)
        client.connection.shutdown(socket.SHUT_RDWR)
        client.connection.close()

        if not silent:
            print(QUIT % (time.strftime('%H:%M:%S'), client.username))
            queue_update_message(channel)

    else:
        channel.clients.remove(client)
        client.connection.shutdown(socket.SHUT_RDWR)
        client.connection.close()

        if not silent:
            if not server_silent:
                print(QUIT % (time.strftime('%H:%M:%S'), client.username))

            server_broadcast(channel, QUIT % (time.strftime('%H:%M:%S'),
                                            client.username),
                                            exclude=client.username)


def queue_update_message(channel) -> None:
    """
    Sends a new queue message to all clients in the channel whenever the queue
    is updated.
    Status: Mine
    Args:
        channel (Channel): The channel to update the queue message for.
    """
    for position, client in enumerate(list(channel.queue.queue)):
        client.connection.send((QUEUE_UPDATE % (time.strftime('%H:%M:%S'),
                                                position)).encode())


def server_broadcast(channel, msg, exclude=None) -> None:
    """
    Broadcast a message to all clients in the channel, can exclude a specific
    client if desired.
    Status: Mine
    Args:
        channel (Channel): The channel to broadcast the message to.
        msg (str): The message to broadcast.
    """
    for client in channel.clients:
        if client.username != exclude:
            client.connection.send(msg.encode())


def send_client(client, channel, msg) -> None:
    """
    Implement file sending function, if args for /send are valid.
    Else print appropriate message and return.
    Status: TODO
    """
    if client.in_queue:
        return

    if client.muted:
        client.connection.send((STILL_MUTED % (time.strftime('%H:%M:%S'),
                                        client.mute_duration)).encode())
        return

    # validate the command structure
    command =  msg.split()

    if len(command) != 3:
        client.connection.send((SEND_ERROR %
                                time.strftime('%H:%M:%S')).encode())
        return

    target = command[1]
    file_path = command[2].strip("\"")

    target_client = find_client(channel, target)
    if not target_client:
        client.connection.send((NOT_HERE % (time.strftime('%H:%M:%S'),
                                    target)).encode())

        if not os.path.exists(file_path):
            client.connection.send((INVALID_FILE %
                                    (time.strftime('%H:%M:%S'),
                                        file_path)).encode())

        return

    # check for file existence
    if not os.path.exists(file_path):
        client.connection.send((INVALID_FILE %
                                (time.strftime('%H:%M:%S'),
                                    file_path)).encode())
        return

    send_file = open(file_path, "r")
    data = send_file.read()

    file_name = file_path.split("/")[-1]
    target_client.connection.send((SEND_FILE % file_name).encode())

    while data:
        target_client.connection.send(str(data).encode())
        data = send_file.read()

    send_file.close()

    client.connection.send((SENT_CLIENT % (time.strftime('%H:%M:%S'),
                                file_path, target)).encode())

    print(SENT_SERVER % (time.strftime('%H:%M:%S'), client.username,
                        file_path, target))


def find_client(channel, username) -> Client:
    """
    Find a client in a channel by their username.
    Status: Mine
    Args:
        channel (Channel): The channel to find the client in.
        username (str): The username of the client to find.
    Returns:
        Client: The client with the given username or None if not found.
    """
    for client in channel.clients:
        if client.username == username:
            return client

    return None


def list_clients(client, channels) -> None:
    """
    V1.1 - List all channels and their capacity.
    Status: TODO
    """
    channel_list = []

    for channel in channels.values():
        channel_list.append(CHANNEL % (channel.name, channel.port,
                                    len(channel.clients), channel.capacity,
                                    channel.queue.qsize()))

    client.connection.send("\n".join(channel_list).encode())


def whisper_client(client, channel, msg) -> None:
    """
    Implement whisper function, if args for /whisper are valid.
    Else print appropriate message and return.
    Status: TODO
    """
    # if in queue, do nothing
    if client.in_queue:
        return

    # if muted, send mute message to the client
    if client.muted:
        client.connection.send((STILL_MUTED % (time.strftime('%H:%M:%S'),
                                        client.mute_duration)).encode())
        return

    # validate the command structure
    command = msg.split()

    if len(command) < 3:
        client.connection.send((WHISPER_ERROR %
                                time.strftime('%H:%M:%S')).encode())
        return

    target = command[1]
    message = SPACE.join(command[2:])

    target_client = find_client(channel, target)
    if not target_client:
        client.connection.send((NOT_HERE % (time.strftime('%H:%M:%S'),
                                            target)).encode())
        return

    target_client.connection.send((WHISPER % (client.username,
                                 time.strftime('%H:%M:%S'), message)).encode())

    print(WHISPER_SERVER % (client.username, target_client.username,
                                        time.strftime('%H:%M:%S'), message))


def switch_channel(client, channel, msg, channels) -> bool:
    """
    Implement channel switching function, if args for /switch are valid.
    Else print appropriate message and return.

    V1.1 - Returns: bool, if the switch is valid or not.
    Status: TODO
    """
    # validate the command structure
    command = msg.split()

    if len(command) != 2:
        client.connection.send((SWITCH_ERROR %
                                time.strftime('%H:%M:%S')).encode())
        return False

    channel_name = command[1]

    # check if the channel exists
    other_channel = channels.get(channel_name, None)

    if not other_channel:
        client.connection.send((INVALID_CHANNEL % (time.strftime('%H:%M:%S'),
                                            channel_name)).encode())
        return False

    # check if there is a client with the same username in the new channel
    if not (check_duplicate_username(client.username, other_channel,
                                            client.connection, close=False)):
        return False

    # remove client and connect to other channel
    print(QUIT % (time.strftime('%H:%M:%S'), client.username), flush=True)
    server_broadcast(channel, QUIT % (time.strftime('%H:%M:%S'),
                                    client.username), exclude=client.username)

    client.connection.send((SWITCH_REQUEST % other_channel.port).encode())
    quit_client(client, channel, silent=True)

    return True


def broadcast_in_channel(client, channel, msg) -> None:
    """
    Broadcast a message to all clients in the channel.
    Status: TODO
    """
    # if in queue, do nothing
    if client.in_queue:
        return

    # if muted, send mute message to the client
    if client.muted:
        client.connection.send((STILL_MUTED % (time.strftime('%H:%M:%S'),
                                            client.mute_duration)).encode())
        return

    print(CLIENT_MESSAGE % (client.username, time.strftime('%H:%M:%S'), msg))

    # broadcast message to all clients in the channel
    for other_client in channel.clients:
        other_client.connection.send((CLIENT_MESSAGE % (client.username,
                                time.strftime('%H:%M:%S'), msg)).encode())


def client_handler(client, channel, channels) -> None:
    """
    V1.1 - Handles incoming messages from a client in a channel. Supports
    commands to quit, send, switch, whisper, and list channels. Manages
    client's mute status and remaining time. Handles client disconnection
    and exceptions during message processing.

    Args:
        client (Client): The client to handle.
        channel (Channel): The channel in which the client is.
        channels (dict): A dictionary of all channels.

    Status: TODO
    """
    while True:
        if client.kicked:
            break
        try:
            msg = client.connection.recv(1024).decode()

            # check message for client commands
            if msg.startswith("/quit"):
                quit_client(client, channel)
                break
            elif msg.startswith("/send"):
                send_client(client, channel, msg)
            elif msg.startswith("/list"):
                list_clients(client, channels)
            elif msg.startswith("/whisper"):
                whisper_client(client, channel, msg)
            elif msg.startswith("/switch"):
                is_valid = switch_channel(client, channel, msg, channels)
                if is_valid:
                    break
                else:
                    continue
            elif msg != "":
                broadcast_in_channel(client, channel, msg)
            if not client.muted:
                client.remaining_time = CLIENT_INTIAL_TIME
        except EOFError:
            continue
        except OSError:
            break
        except Exception as error:
            print(CLIENT_HANDLER_ERROR % error)
            quit_client(client, channel)
            break


def check_duplicate_username(username, channel, conn, close=True) -> bool:
    """
    Check if a username is valid, that is return false if it is already used
    and true if not. Also send the correct message to the client.
    Status: TODO
    """
    all_clients = channel.clients + list(channel.queue.queue)
    for client in all_clients:
        if client.username == username:
            conn.send((DUPLICATE_USER % (time.strftime('%H:%M:%S'),
                                        channel.name, username)).encode())
            if close:
                conn.send(QUIT_REQUEST.encode())
                client.connection.shutdown(socket.SHUT_RDWR)
                conn.close()

            return False

    return True


def position_client(channel, conn, username, new_client) -> None:
    """
    Place a client in a channel or queue based on the channel's capacity.
    Status: TODO
    """
    conn.send((WELCOME_MESSAGE % (time.strftime('%H:%M:%S'),
                                  channel.name, username)).encode())

    if len(channel.clients) < channel.capacity:
        channel.clients.append(new_client)
        new_client.in_queue = False
        new_client.remaining_time = CLIENT_INTIAL_TIME
        server_broadcast(channel, JOINED_CLIENT % (time.strftime('%H:%M:%S'),
                                                   username))
        print(JOINED_SERVER % (time.strftime('%H:%M:%S'), username,
                                            channel.name))

    else:
        channel.queue.put(new_client)
        new_client.in_queue = True
        position = channel.queue.qsize() - 1
        conn.send((WELCOME_MESSAGE_QUEUE % (time.strftime('%H:%M:%S'),
                   channel.name, username)).encode())
        conn.send((QUEUE_UPDATE % (time.strftime('%H:%M:%S'),
                                   position)).encode())


def channel_handler(channel, channels) -> None:
    """
    Starts a chat server, manage channels, respective queues, and incoming
    clients. This initiates different threads for chanel queue processing and
    client handling.
    Status: Given
    Args:
        channel (Channel): The channel for which to start the server.
    Raises:
        EOFError: If there is an error in the client-server communication.
    """
    # Initialize server socket, bind, and listen
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind(("localhost", channel.port))
    server_socket.listen(channel.capacity)

    # launch a thread to process client queue
    queue_thread = threading.Thread(target=process_queue, args=(channel,))
    queue_thread.start()

    while True:
        try:
            # accept a client connection
            conn, addr = server_socket.accept()
            username = conn.recv(1024).decode()

            # check duplicate username in channel and channel's queue
            is_valid = check_duplicate_username(username, channel, conn)
            if not is_valid: continue

            time.sleep(0.1)
            new_client = Client(username, conn, addr)

            # position client in channel or queue
            position_client(channel, conn, username, new_client)

            # Create a client thread for each connected client, whether they
            # are in the channel or queue
            client_thread = threading.Thread(target=client_handler,
                                          args=(new_client, channel, channels))
            client_thread.start()

        except EOFError:
            continue


def remove_item(q, item_to_remove) -> queue.Queue:
    """
    Remove item from queue
    Status: Given
    Args:
        q (queue.Queue): The queue to remove the item from.
        item_to_remove (Client): The item to remove from the queue.
    Returns:
        queue.Queue: The queue with the item removed.
    """
    new_q = queue.Queue()
    while not q.empty():
        current_item = q.get()
        if current_item != item_to_remove:
            new_q.put(current_item)

    return new_q


def process_queue(channel) -> None:
    """
    Processes the queue of clients for a channel in an infinite loop. If the
    channel is not full, it dequeues a client, adds them to the channel, and
    updates their status. It then sends updates to all clients in the channel
    and queue. The function handles EOFError exceptions and sleeps for 1 second
    between iterations.
    Status: TODO
    Args:
        channel (Channel): The channel whose queue to process.
    Returns:
        None
    """
    while True:
        try:
            if not channel.queue.empty() \
                and len(channel.clients) < channel.capacity:

                # Dequeue a client from the queue and add them to the channel
                client = channel.queue.get()
                position_client(channel, client.connection, client.username,
                                client)

                # Update the queue messages for remaining clients in the queue
                queue_update_message(channel)

                # Sleep for 1 second
                time.sleep(0.99)

        except EOFError:
            continue


def kick_user(msg, channels) -> None:
    """
    Implement /kick function
    Status: TODO
    Args:
        command (str): The command to kick a user from a channel.
        channels (dict): A dictionary of all channels.
    Returns:
        None
    """
    # validate command structure
    command = msg.split()

    if len(command) != 3:
        print(KICK_ERROR % time.strftime('%H:%M:%S'))
        return

    channel_name = command[1]
    username = command[2]

    # check if the channel exists in the dictionary
    channel = channels.get(channel_name, None)

    if not channel:
        print(INVALID_CHANNEL % (time.strftime('%H:%M:%S'),
                                            channel_name))
        return

    # if channel exists, check if the user is in the channel
    client = find_client(channel, username)
    if not client:
        print(NOT_HERE % (time.strftime('%H:%M:%S'), username))
        return

    # if user is in the channel, kick the user
    client.kicked = True
    quit_client(client, channel, server_silent=True)
    print(KICKED % (time.strftime('%H:%M:%S'), username))


def empty(msg, channels) -> None:
    """
    Implement /empty function
    Status: TODO
    Args:
        msg (str): The command to empty a channel.
        channels (dict): A dictionary of all channels.
    """
    # validate the command structure
    command = msg.split()

    if len(command) != 2:
        print(EMPTY_ERROR % time.strftime('%H:%M:%S'))
        return

    channel_name = command[1]

    # check if the channel exists in the server
    channel = channels.get(channel_name, None)

    if not channel:
        print(INVALID_CHANNEL % (time.strftime('%H:%M:%S'),
                                            channel_name))
        return

    # close connections for all clients in channel and queue
    for client in channel.clients:
        quit_client(client, channel, silent=True)

    print(CHANNEL_EMPTIED % (time.strftime('%H:%M:%S'),
                                        channel_name))


def mute_user(msg, channels) -> None:
    """
    Implement /mute function
    Status: TODO
    Args:
        msg (str): The command to mute a user in a channel.
        channels (dict): A dictionary of all channels.
    """
    # validate the command structure
    command = msg.split()

    if len(command) != 4:
        print(MUTE_ERROR % time.strftime('%H:%M:%S'))
        return

    channel_name = command[1]
    username = command[2]
    mute_time = int(command[3])

    # check if the channel exists in the server
    channel = channels.get(channel_name, None)

    # if the channel exists, check if the user is in the channel
    if channel:
        client = find_client(channel, username)

    if not channel or not client:
        print(NOT_HERE % (time.strftime('%H:%M:%S'), username))

        if mute_time <= 0:
            print(INVALID_MUTE_TIME % time.strftime('%H:%M:%S'))

        return

    # check if the mute time is valid
    if mute_time <= 0:
        print(INVALID_MUTE_TIME % time.strftime('%H:%M:%S'))
        return

    # if user is in the channel, mute it and send messages to all clients
    client.muted = True
    client.mute_duration = mute_time

    client.connection.send((MUTED_CLIENT % 
                            (time.strftime('%H:%M:%S'), mute_time)).encode())

    server_broadcast(channel, MUTED_CLIENTS % (time.strftime('%H:%M:%S'),
                                        username, mute_time), exclude=username)

    print(MUTED_SERVER % (time.strftime('%H:%M:%S'), username,
                                    mute_time))


def shutdown(channels) -> None:
    """
    Implement /shutdown function
    Status: TODO
    Args:
        channels (dict): A dictionary of all channels.
    """
    # close connections of all clients in all channels and exit the server
    for channel in channels:
        channel = channels[channel]
        all_clients = channel.clients + list(channel.queue.queue)
        for client in all_clients:
            quit_client(client, channel, silent=True)

    # end of code insertion, keep the os._exit(0) as it is
    os._exit(0)


def server_commands(channels) -> None:
    """
    Implement commands to kick a user, empty a channel, mute a user, and
    shutdown server. Each command has its own validation and error handling.
    Status: Given
    Args:
        channels (dict): A dictionary of all channels.
    Returns:
        None
    """
    while True:
        try:
            command = input()
            if command.startswith("/kick"):
                kick_user(command, channels)
            elif command.startswith("/empty"):
                empty(command, channels)
            elif command.startswith("/mute"):
                mute_user(command, channels)
            elif command == "/shutdown":
                shutdown(channels)
            else:
                continue
        except EOFError:
            continue
        except Exception as e:
            print(f"{e}")
            sys.exit(1)


def check_inactive_clients(channels) -> None:
    """
    Continuously manages clients in all channels. Checks if a client is muted,
    in queue, or has run out of time. If a client's time is up, they are
    removed from the channel and their connection is closed. A server message
    is sent to all clients in the channel. The function also handles EOFError
    exceptions.
    Status: TODO
    Args:
        channels (dict): A dictionary of all channels.
    """
    while True:
        # parse through all the clients in all the channels
        try:
            for channel_name in channels:
                channel = channels[channel_name]

                for client in channel.clients:
                    # if client is muted or in queue, do nothing

                    if client.muted or client.in_queue:
                        continue

                    if client.remaining_time <= 0:
                        # remove client from the channel and close connection
                        quit_client(client, channel, silent=True)
                        server_broadcast(channel, AFK %
                                (time.strftime('%H:%M:%S'), client.username))
                        print(AFK %
                              (time.strftime('%H:%M:%S'), client.username))

                    else:
                        client.remaining_time -= 1

            time.sleep(0.99)
        except EOFError:
            continue


def handle_mute_durations(channels) -> None:
    """
    Continuously manages the mute status of clients in all channels. If a
    client's mute duration has expired, their mute status is lifted. If a
    client is still muted, their mute duration is decremented. The function
    sleeps for 0.99 seconds between iterations and handles EOFError exceptions.
    Status: Given
    Args:
        channels (dict): A dictionary of all channels.
    """
    while True:
        try:
            for channel_name in channels:
                channel = channels[channel_name]
                for client in channel.clients:
                    if client.mute_duration <= 0:
                        client.muted = False
                        client.mute_duration = 0
                    if client.muted and client.mute_duration > 0:
                        client.mute_duration -= 1
            time.sleep(0.99)
        except EOFError:
            continue


def main():
    try:
        if len(sys.argv) != 2:
            print("Usage: python3 chatserver.py configfile")
            sys.exit(1)

        config_file = sys.argv[1]

        # parsing and creating channels
        parsed_lines = parse_config(config_file)
        channels = get_channels_dictionary(parsed_lines)

        # creating individual threads to handle channels connections
        for _, channel in channels.items():
            thread = threading.Thread(
                target=channel_handler, args=(channel, channels))
            thread.start()

        server_commands_thread = threading.Thread(
            target=server_commands, args=(channels,))
        server_commands_thread.start()

        inactive_clients_thread = threading.Thread(
            target=check_inactive_clients, args=(channels,))
        inactive_clients_thread.start()

        mute_duration_thread = threading.Thread(
            target=handle_mute_durations, args=(channels,))
        mute_duration_thread.start()

    except KeyboardInterrupt:
        print("Ctrl + C Pressed. Exiting...")
        os._exit(0)


if __name__ == "__main__":
    main()
