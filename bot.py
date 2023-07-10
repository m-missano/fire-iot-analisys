import telebot

BOT_TOKEN = "BOT_TOKEN" # Alterar BOT_TOKEN

bot = telebot.TeleBot(BOT_TOKEN)

# Envia mensagem de aviso de risco de incêndio    
def aviso(mensagem):
    chat_id = # Inserir chatid do bot
    bot.send_message(chat_id, mensagem)
