# Kademlia-Distributed-Hash-Table
A decentralized peer-to-peer computer network DHT, using UDP datagram python sockets

Information is shared via UDP lookups that contain JSON objects.

Based on the information passed in the JSON objects, different protocols will be invoked.

Every time a message is passed, my DHT 'ring' will stabilize, to ensure that each node has the correct predecessor and successor.
