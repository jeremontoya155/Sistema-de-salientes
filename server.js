/**
 * server.js - Servidor Express para el Bot de Instagram con IA
 *
 * Descripción: Provee una interfaz web (EJS) para configurar y lanzar el bot.
 * Versión: 3.0
 */

const express = require('express');
const path = require('path');
const multer = require('multer');
const dotenv = require('dotenv');
const { runBotLogic } = require('./botLogic'); // Importamos la lógica del bot

dotenv.config(); // Carga variables de .env

const app = express();
const port = process.env.PORT || 3000;

// --- Configuración de Multer para subida de archivos ---
// Guardaremos los archivos CSV subidos en una carpeta 'uploads'
const storage = multer.diskStorage({
    destination: function (req, file, cb) {
        cb(null, 'uploads/'); // Asegúrate de que la carpeta 'uploads' exista
    },
    filename: function (req, file, cb) {
        // Nombre de archivo único para evitar colisiones
        const uniqueSuffix = Date.now() + '-' + Math.round(Math.random() * 1E9);
        cb(null, file.fieldname + '-' + uniqueSuffix + path.extname(file.originalname));
    }
});

const upload = multer({
    storage: storage,
    fileFilter: (req, file, cb) => {
        // Validar que sea un archivo CSV
        if (path.extname(file.originalname).toLowerCase() === '.csv') {
            cb(null, true);
        } else {
            cb(new Error('Solo se permiten archivos CSV.'), false);
        }
    }
}); // 'csvFile' será el nombre del campo en el formulario HTML

// --- Configuración de Express ---
app.set('view engine', 'ejs'); // Usar EJS como motor de plantillas
app.set('views', path.join(__dirname, 'views')); // Directorio de las vistas
app.use(express.urlencoded({ extended: true })); // Para parsear datos de formularios
app.use(express.static(path.join(__dirname, 'public'))); // Carpeta para archivos estáticos (CSS, JS del cliente) - Opcional

// --- Estado del Bot ---
// Para evitar ejecuciones múltiples (simple - podría mejorarse con colas de trabajo)
let isBotRunning = false;

// --- Rutas ---

// Ruta principal: Muestra el formulario de configuración
app.get('/', (req, res) => {
    if (isBotRunning) {
        res.render('index', {
            message: { type: 'info', text: 'El bot ya está en ejecución. Espera a que termine.' },
            isRunning: true,
            env: process.env // Pasamos variables de entorno (solo las necesarias y seguras)
        });
    } else {
        res.render('index', {
            message: null, // Sin mensaje inicial
            isRunning: false,
            env: process.env
        });
    }
});

// Ruta para iniciar el bot (POST desde el formulario)
app.post('/start', upload.single('csvFile'), async (req, res) => {
    if (isBotRunning) {
        return res.status(429).render('index', {
             message: { type: 'error', text: 'El bot ya está en ejecución. No se puede iniciar otro proceso.' },
             isRunning: true,
             env: process.env
        });
    }

    if (!req.file) {
        return res.status(400).render('index', {
            message: { type: 'error', text: 'Error: No se subió ningún archivo CSV.' },
            isRunning: false,
            env: process.env
        });
    }

    // Extraer datos del formulario (req.body) y archivo (req.file)
    const {
        sessionId,
        proxy,
        csvType,
        filterKeywords,
        context,
        maxMessages,
        baseDelay
    } = req.body;
    const csvPath = req.file.path; // Ruta al archivo CSV subido

    // Validación básica de entradas
    if (!sessionId || !context || !csvPath || !csvType) {
        return res.status(400).render('index', {
            message: { type: 'error', text: 'Error: Faltan campos requeridos (Session ID, Contexto, Archivo CSV, Tipo CSV).' },
            isRunning: false,
            env: process.env
         });
    }
    if (csvType === 'followers' && !filterKeywords) {
         console.warn("Advertencia: Se seleccionó tipo 'followers' pero no se ingresaron palabras clave para filtrar.");
         // Podrías decidir si esto es un error o simplemente no filtrar
     }


    // Preparar la configuración para la lógica del bot
    const config = {
        instagramSessionId: sessionId.trim(),
        proxy: proxy ? proxy.trim() : null,
        openaiApiKey: process.env.OPENAI_API_KEY, // Desde .env
        mongoUri: process.env.MONGO_URI,         // Desde .env
        csvPath: csvPath,
        csvType: csvType,
        filterKeywords: filterKeywords ? filterKeywords.trim() : '',
        messageContext: context.trim(),
        maxMessages: parseInt(maxMessages) || 0, // 0 significa todos
        baseDelaySeconds: parseInt(baseDelay) || 90, // Default seguro si no se provee o es inválido
    };

    // --- Validación adicional ---
    if (!config.openaiApiKey || !config.mongoUri) {
       console.error('❌ Faltan variables de entorno requeridas (OPENAI_API_KEY, MONGO_URI)');
       return res.status(500).render('index', {
           message: { type: 'error', text: 'Error de configuración del servidor. Faltan claves API o URI de DB.' },
           isRunning: false,
           env: process.env
        });
    }
     if (config.baseDelaySeconds < 30) {
         console.warn(`⚠️ Retardo base (${config.baseDelaySeconds}s) es muy bajo. Aumentando a 30s por seguridad.`);
         config.baseDelaySeconds = 30;
     }


    // Marcamos el bot como en ejecución
    isBotRunning = true;
    console.log("🚀 Iniciando el bot con la configuración recibida...");

    // Enviamos una respuesta inmediata al usuario indicando que el proceso comenzó
    // El bot se ejecutará en segundo plano. Los logs se verán en la consola del servidor y en ai_bot.log
    res.render('index', {
        message: { type: 'success', text: `Bot iniciado con el archivo ${req.file.originalname}. Revisa los logs del servidor y ai_bot.log para ver el progreso.` },
        isRunning: true, // Indica que el bot está corriendo
        env: process.env
    });

    // Ejecutar la lógica principal del bot de forma asíncrona
    // Usamos then/catch para manejar el final y errores, y para resetear el flag isBotRunning
    runBotLogic(config)
        .then(() => {
            console.log("✅ Proceso del bot completado exitosamente.");
        })
        .catch((error) => {
            console.error("❌ Error fatal durante la ejecución del bot:", error);
        })
        .finally(() => {
            console.log("🏁 Bot detenido.");
            isBotRunning = false; // Permite una nueva ejecución
            // Opcional: Eliminar el archivo CSV subido después de usarlo
            const fs = require('fs').promises;
            fs.unlink(config.csvPath).catch(err => console.error(`Error al eliminar ${config.csvPath}:`, err));
        });

});

// Middleware de manejo de errores (específico para Multer y otros)
app.use((err, req, res, next) => {
    if (err instanceof multer.MulterError) {
        // Un error de Multer ocurrió durante la subida.
        console.error("Error de Multer:", err);
        res.status(400).render('index', { message: { type: 'error', text: `Error al subir archivo: ${err.message}` }, isRunning: isBotRunning, env: process.env });
    } else if (err) {
        // Otro tipo de error
        console.error("Error inesperado:", err);
         res.status(500).render('index', { message: { type: 'error', text: `Error inesperado: ${err.message}` }, isRunning: isBotRunning, env: process.env });
    } else {
        next();
    }
});


// --- Iniciar Servidor ---
// Crear carpeta uploads si no existe
const fs = require('fs');
if (!fs.existsSync('./uploads')) {
    fs.mkdirSync('./uploads');
    console.log("📁 Carpeta 'uploads' creada.");
}


app.listen(port, () => {
    console.log(`🌐 Servidor web escuchando en http://localhost:${port}`);
    console.log(`🔑 Asegúrate de tener OPENAI_API_KEY y MONGO_URI en tu archivo .env`);
});