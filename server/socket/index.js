const express = require("express");
const { Server } = require("socket.io");
const http = require("http");
const mongoose = require("mongoose");
const getUserDetailsFromToken = require("../helpers/getUserDetailsFrom Token");
const UserModel = require("../models/UserModel");
const {
  ConversationModel,
  MessageModel,
} = require("../models/ConversationModel");
const getConversation = require("../helpers/getConversation");

const app = express();

/***socket connection */
const server = http.createServer(app);
const io = new Server(server, {
  cors: {
    origin: process.env.FRONTEND_URL,
    credentials: true,
  },
});

/***
 * socket running at http://localhost:8080/
 */

//online user
const onlineUser = new Set();

io.on("connection", async (socket) => {
  console.log("Connected User:", socket.id);

  const token = socket.handshake.auth.token;

  // Validate token and get user details
  const user = await getUserDetailsFromToken(token);

  if (!user || !user._id) {
    console.error("Authentication failed: Invalid token or user not found");
    return;
  }

  // Create a room for the user
  socket.join(user._id.toString());
  onlineUser.add(user._id.toString());

  // Emit online users to all clients
  io.emit("onlineUser", Array.from(onlineUser));

  socket.on("message-page", async (userId) => {
    if (!userId || !mongoose.Types.ObjectId.isValid(userId)) {
      console.error("Invalid userId received in message-page:", userId);
      return;
    }

    console.log("userId", userId);
    const userDetails = await UserModel.findById(userId).select("-password");

    const payload = {
      _id: userDetails?._id,
      name: userDetails?.name,
      email: userDetails?.email,
      profile_pic: userDetails?.profile_pic,
      online: onlineUser.has(userId),
    };
    socket.emit("message-user", payload);

    // Get previous messages
    const getConversationMessage = await ConversationModel.findOne({
      $or: [
        { sender: user._id, receiver: userId },
        { sender: userId, receiver: user._id },
      ],
    })
      .populate("messages")
      .sort({ updatedAt: -1 });

    socket.emit("message", getConversationMessage?.messages || []);
  });

  // New message event
  socket.on("new message", async (data) => {
    if (!data?.sender || !data?.receiver) {
      console.error("Invalid sender or receiver in new message event.");
      return;
    }

    let conversation = await ConversationModel.findOne({
      $or: [
        { sender: data.sender, receiver: data.receiver },
        { sender: data.receiver, receiver: data.sender },
      ],
    });

    // If conversation doesn't exist, create a new one
    if (!conversation) {
      const createConversation = await new ConversationModel({
        sender: data.sender,
        receiver: data.receiver,
      });
      conversation = await createConversation.save();
    }

    const message = new MessageModel({
      text: data.text,
      imageUrl: data.imageUrl,
      videoUrl: data.videoUrl,
      msgByUserId: data.msgByUserId,
    });
    const saveMessage = await message.save();

    // Update conversation with the new message
    const updateConversation = await ConversationModel.updateOne(
      { _id: conversation._id },
      {
        $push: { messages: saveMessage._id },
      }
    );

    // Get updated conversation and emit messages to both users
    const getConversationMessage = await ConversationModel.findOne({
      $or: [
        { sender: data.sender, receiver: data.receiver },
        { sender: data.receiver, receiver: data.sender },
      ],
    })
      .populate("messages")
      .sort({ updatedAt: -1 });

    io.to(data.sender).emit("message", getConversationMessage?.messages || []);
    io.to(data.receiver).emit(
      "message",
      getConversationMessage?.messages || []
    );

    // Send updated conversation data
    const conversationSender = await getConversation(data.sender);
    const conversationReceiver = await getConversation(data.receiver);

    io.to(data.sender).emit("conversation", conversationSender);
    io.to(data.receiver).emit("conversation", conversationReceiver);
  });

  // Sidebar event to get conversation list
  socket.on("sidebar", async (currentUserId) => {
    if (!currentUserId || !mongoose.Types.ObjectId.isValid(currentUserId)) {
      console.error(
        "Invalid currentUserId received in sidebar:",
        currentUserId
      );
      return;
    }

    console.log("current user", currentUserId);

    const conversation = await getConversation(currentUserId);
    socket.emit("conversation", conversation);
  });

  // Seen message event
  socket.on("seen", async (msgByUserId) => {
    if (!msgByUserId || !mongoose.Types.ObjectId.isValid(msgByUserId)) {
      console.error("Invalid msgByUserId received in seen event:", msgByUserId);
      return;
    }

    let conversation = await ConversationModel.findOne({
      $or: [
        { sender: user._id, receiver: msgByUserId },
        { sender: msgByUserId, receiver: user._id },
      ],
    });

    const conversationMessageId = conversation?.messages || [];

    const updateMessages = await MessageModel.updateMany(
      { _id: { $in: conversationMessageId }, msgByUserId: msgByUserId },
      { $set: { seen: true } }
    );

    // Send updated conversation to both users
    const conversationSender = await getConversation(user._id.toString());
    const conversationReceiver = await getConversation(msgByUserId);

    io.to(user._id.toString()).emit("conversation", conversationSender);
    io.to(msgByUserId).emit("conversation", conversationReceiver);
  });

  // User disconnect event
  socket.on("disconnect", () => {
    onlineUser.delete(user._id.toString());
    console.log("Disconnected user:", socket.id);
  });
});

module.exports = {
  app,
  server,
};
