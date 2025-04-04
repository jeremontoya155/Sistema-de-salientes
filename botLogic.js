/**
 * botLogic.js - Lógica principal del Bot de Instagram con IA y MongoDB
 *
 * Descripción: Contiene las funciones para conectar, generar mensajes, enviar y registrar.
 *              Se ejecuta al ser llamado desde server.js con la configuración necesaria.
 * Versión: 3.0
 */

const { IgApiClient } = require('instagram-private-api');
const { OpenAI } = require('openai');
const { MongoClient } = require('mongodb');
const fs = require('fs');
const fsp = require('fs').promises;
const path = require('path');
const csv = require('csv-parser');

// --- Variables globales dentro del módulo (se inicializarán en runBotLogic) ---
let igClient;
let openai;
let mongoClient;
let mongoCollection;
let botUsername; // Nombre de usuario del bot logueado

// --- Funciones de utilidad (como las tenías antes) ---
function sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

function getRandomDelay(min, max) {
    return Math.floor(Math.random() * (max - min + 1)) + min;
}

async function logAction(message) {
    const timestamp = new Date().toISOString();
    const logMessage = `[${timestamp}] ${message}\n`;
    try {
        await fsp.appendFile('ai_bot.log', logMessage, 'utf8');
    } catch (logError) {
        console.error("⚠️ Error al escribir en ai_bot.log:", logError.message);
    }
    console.log(message); // También loguea en consola
}

function formatDateTime(date) {
    const yyyy = date.getFullYear();
    const mm = String(date.getMonth() + 1).padStart(2, '0');
    const dd = String(date.getDate()).padStart(2, '0');
    const hh = String(date.getHours()).padStart(2, '0');
    const mi = String(date.getMinutes()).padStart(2, '0');
    const ss = String(date.getSeconds()).padStart(2, '0');
    return `${yyyy}-${mm}-${dd} ${hh}:${mi}:${ss}`;
}

// --- Conexión a MongoDB ---
async function connectMongo(mongoUri) {
    if (mongoClient && mongoClient.topology && mongoClient.topology.isConnected()) {
        logAction('ℹ️ Ya conectado a MongoDB.');
        return mongoCollection;
    }
    try {
        mongoClient = new MongoClient(mongoUri);
        await mongoClient.connect();
        logAction('✅ Conectado a MongoDB');
        mongoCollection = mongoClient.db('instagram_bot').collection('historial_mensajes');
        return mongoCollection;
    } catch (error) {
        logAction(`❌ Error conectando a MongoDB: ${error.message}`);
        throw new Error(`Error fatal conectando a MongoDB: ${error.message}`); // Propaga el error
    }
}

// --- Generación de mensajes con IA (sin cambios significativos) ---
async function generateAIMessage(targetUsername, profileData, context) {
    // ...(código idéntico al que tenías en tu script original)...
    // Asegúrate de que 'openai' esté inicializado antes de llamar esta función
    if (!openai) throw new Error("Cliente OpenAI no inicializado.");

    let detailedUserData = {};
    try {
        // Re-usar igClient inicializado globalmente
        detailedUserData = await igClient.user.info(profileData.pk);
    } catch (infoError) {
        logAction(`⚠️ No se pudieron obtener detalles completos para @${targetUsername}. Usando datos básicos.`);
        detailedUserData = {
            full_name: profileData.full_name,
            biography: profileData.biography,
            follower_count: profileData.follower_count,
        };
    }

    try {
      const prompt = `Eres un asistente de redes sociales amigable y natural. Genera un mensaje corto y único (máximo 2 frases, idealmente 1) para enviar por DM de Instagram a @${targetUsername}.
Aquí hay información sobre el usuario (puede ser limitada):
- Nombre Completo: ${detailedUserData.full_name || 'No disponible'}
- Biografía: ${detailedUserData.biography || 'No disponible'}
- Seguidores: ${detailedUserData.follower_count || 'N/A'}

Contexto para el mensaje (objetivo): ${context}

Reglas IMPORTANTES para el mensaje:
1.  **Naturalidad Extrema:** Debe sonar como si lo escribiera una persona real, no un bot. Evita formalismos excesivos o lenguaje de marketing obvio.
2.  **Brevedad:** Una frase es ideal, máximo dos. Los DMs largos suelen ignorarse.
3.  **Evita Saludos Genéricos:** NO uses "Hola [nombre]", "Hola @${targetUsername}", "Qué tal?", "Espero que estés bien". Ve directo al punto de forma amigable.
4.  **Personalización Sutil (Si es posible):** Si hay algo *interesante* y *no genérico* en el nombre o biografía, haz una referencia MUY SUTIL. Si no hay nada destacable, no fuerces la personalización. Ejemplo sutil: si la bio dice "Amante de los gatos", podrías empezar con "Vi que te gustan los gatos!..." (solo si encaja con el contexto). Si la bio está vacía o es genérica, ignórala.
5.  **Enfocado en el Contexto:** El mensaje debe reflejar claramente el objetivo proporcionado en el contexto.
6.  **Llamada a la Acción Implícita o Suave (Opcional):** Puede terminar con algo que invite a la interacción relacionada con el contexto, pero sin ser agresivo.
7.  **Variedad:** Asegúrate de que cada mensaje sea distinto a los anteriores generados en esta sesión.
8.  **Tono:** Amigable, cercano y respetuoso.

Ejemplo de MALA salida: "Hola ${detailedUserData.full_name}, vi tu perfil y quería invitarte a mi evento." (Demasiado genérico y formal)
Ejemplo de BUENA salida (si el contexto es invitar a un evento de música): "Che, vi que te gusta la música, te paso la data de un evento que te puede copar!"
Ejemplo de BUENA salida (si el contexto es ofrecer un servicio de diseño): "Cómo va? Vi tu perfil, si andás necesitando una mano con diseño gráfico, chiflame!"

Genera SOLO el texto del mensaje final.`;


        const response = await openai.chat.completions.create({
            model: 'gpt-4o-mini', // O el modelo que prefieras/tengas acceso
            messages: [{ role: 'user', content: prompt }],
            temperature: 0.8,
            max_tokens: 70,
            n: 1,
            stop: ["\n\n"],
        });

        let messageContent = response.choices[0].message.content.trim();
        messageContent = messageContent.replace(/^"|"$/g, '');
        messageContent = messageContent.replace(/\\n/g, ' ');
        messageContent = messageContent.replace(/\s+/g, ' ');

        const sentences = messageContent.split(/(?<=[.?!])\s+/);
        if (sentences.length > 2) {
            messageContent = sentences.slice(0, 2).join(' ');
        }

        if (!messageContent || messageContent.length < 10) {
            logAction(`⚠️ Mensaje generado por IA demasiado corto o vacío para @${targetUsername}. Se usará un mensaje de respaldo.`);
            return `Qué tal? Vi tu perfil y pensé que te podría interesar esto relacionado con: ${context}. Saludos!`;
        }

        return messageContent;
    } catch (error) {
        logAction(`❌ Error generando mensaje con IA para @${targetUsername}: ${error.message}`);
        // Considerar si devolver null o un mensaje de fallback aquí podría ser mejor
        // dependiendo de si quieres registrar el fallo o intentar enviar algo genérico
        if (error.response && error.response.status === 429) {
            logAction("🚦 Límite de tasa de OpenAI alcanzado. Esperando 60 segundos...");
            await sleep(60000);
            // Podrías reintentar aquí, pero por simplicidad devolvemos el fallback
        }
        return `Hola! Te contacto por esto: ${context}. Puede que te interese.`; // Fallback
    }
}


// --- Manejo de CSV (sin cambios significativos) ---
async function readCSV(filePath) {
    // ...(código idéntico al que tenías en tu script original)...
     return new Promise((resolve, reject) => {
        logAction(`📄 Leyendo archivo CSV: ${filePath}`);
        const results = [];
        fs.createReadStream(filePath)
          .pipe(csv({ /* separator: ';' */ })) // Detecta automáticamente o especifica si es necesario
          .on('data', (data) => {
               const cleanedData = {};
               for (const key in data) {
                   const cleanedKey = key.trim().toLowerCase().replace(/\s+/g, '');
                   if (cleanedKey === 'username') {
                       cleanedData['userName'] = data[key];
                   } else if (cleanedKey === 'fullname') {
                       cleanedData['fullName'] = data[key];
                   } else if (cleanedKey === 'id') {
                        cleanedData['id'] = data[key];
                   } else {
                        cleanedData[cleanedKey] = data[key];
                    }
               }
               if (!('userName' in cleanedData)) cleanedData.userName = '';
               if (!('fullName' in cleanedData)) cleanedData.fullName = '';
               results.push(cleanedData);
          })
          .on('end', () => {
            logAction(`✅ CSV leído. ${results.length} filas encontradas.`);
            if (results.length > 0) {
                logAction(`🔍 Cabeceras detectadas (limpias): ${Object.keys(results[0]).join(', ')}`);
            }
            resolve(results);
          })
          .on('error', (err) => {
             logAction(`❌ Error leyendo CSV: ${err.message}`);
             reject(new Error(`Error leyendo CSV: ${err.message}`)); // Propagar error
          });
      });
}

// --- Filtrar Cuentas (adaptado para recibir keywords como string) ---
async function filterAccountsByFullName(accounts, filterKeywordsString) {
    // ...(código similar, adaptado para recibir las keywords del formulario)...
    if (!filterKeywordsString || !filterKeywordsString.trim()) {
        logAction('ℹ️ No se aplicará filtro por fullName (no se proporcionaron palabras clave).');
        return accounts;
    }

    const keywords = filterKeywordsString
        .toLowerCase()
        .split(',')
        .map((k) => k.trim())
        .filter((k) => k);

    if (!keywords.length) {
        logAction('ℹ️ No se ingresaron palabras clave válidas para filtrar.');
        return accounts;
    }

    logAction(`🔎 Filtrando ${accounts.length} cuentas por fullName usando las claves: ${keywords.join(', ')}`);

    const filtered = accounts.filter((account) => {
        const fullNameLower = (account.fullName || '').toLowerCase();
        if (!fullNameLower) {
            return false; // No se puede filtrar si no hay fullName
        }
        // Verifica si ALGUNA de las keywords está incluida en el fullName
        return keywords.some((keyword) => fullNameLower.includes(keyword));
    });

    logAction(`📊 ${filtered.length} cuentas coinciden con el filtro.`);
    return filtered;
}


// --- Envío de mensajes (sin cambios significativos, usa igClient global) ---
async function sendMessage(userId, message) {
    // ...(código idéntico al que tenías)...
    if (!igClient) throw new Error("Cliente Instagram no inicializado.");
    try {
        const thread = await igClient.entity.directThread([userId.toString()]);
        await thread.broadcastText(message);
        return true; // Éxito
    } catch (error) {
        let shouldStop = false;
        let waitTime = 0; // Tiempo de espera en ms

        if (error.response && error.response.statusCode === 400 && error.message.includes('Cannot message users you dont follow')) {
            logAction(`🚫 No se puede enviar mensaje a ${userId}: No sigues a este usuario, tiene DMs restringidos o cuenta inexistente.`);
        } else if (error.response?.body?.message?.includes('rate limited') || error.response?.statusCode === 429) {
            waitTime = 5 * 60 * 1000; // 5 minutos
            logAction(`⏱️ Límite de tasa alcanzado intentando enviar a ${userId}. Esperando ${waitTime / 60000} minutos...`);
        } else if (error.response?.body?.message?.includes('checkpoint_required')) {
            logAction(`🚨 ¡CHECKPOINT REQUERIDO! Deteniendo el script. Resuelve el checkpoint en Instagram.`);
            shouldStop = true; // Indicar que hay que detener el proceso
        } else if (error.message?.includes('login_required') || error.response?.statusCode === 403) {
             logAction(`🔒 ¡LOGIN REQUERIDO o Prohibido! Session ID inválido/expirado o acción bloqueada. Deteniendo.`);
             shouldStop = true;
        } else {
            logAction(`❌ Error enviando mensaje a ${userId}: ${error.message}`);
            // Podrías añadir una pausa corta aquí también por si acaso
            waitTime = getRandomDelay(5000, 15000); // Espera corta tras error desconocido
            logAction(`⏳ Pausa corta de ${Math.round(waitTime / 1000)}s tras error inesperado.`);
        }

        if (waitTime > 0) {
            await sleep(waitTime);
        }

        // Devolvemos un objeto indicando el resultado y si hay que parar
        return { success: false, shouldStop: shouldStop };
    }
    // Si llegamos aquí, el mensaje se envió
    return { success: true, shouldStop: false };
}


// --- Configuración Instagram (adaptada para recibir Session ID y Proxy) ---
async function setupInstagram(sessionId, proxyUrl) {
    // ...(código similar, pero usa los parámetros)...
    try {
        igClient = new IgApiClient(); // Crear nueva instancia

        if (proxyUrl) {
            igClient.state.proxyUrl = proxyUrl;
            logAction(`🌍 Usando proxy: ${proxyUrl}`);
        }

        igClient.state.generateDevice(sessionId); // Usar el sessionId proporcionado

        // Necesario para simular un login válido con Session ID
        // Importante: El sessionId debe ser válido y reciente
        await igClient.state.deserializeCookieJar(JSON.stringify({
            "version": "tough-cookie@4.1.3", // O la versión que corresponda
            "storeType": "MemoryCookieStore",
            "rejectPublicSuffixes": true,
            "cookies": [{
                "key": "sessionid",
                "value": sessionId,
                "domain": "instagram.com",
                "path": "/",
                "secure": true,
                "httpOnly": true,
                "creation": new Date().toISOString(),
                "lastAccessed": new Date().toISOString()
            }]
        }));

        // Opcional: Simular flujos puede ayudar a evitar detecciones
        // await igClient.simulate.preLoginFlow();

        // Verificar la sesión obteniendo datos del usuario actual
        const user = await igClient.account.currentUser();
        if (!user || !user.pk) {
             throw new Error("No se pudo obtener información del usuario actual. Session ID podría ser inválido.");
        }
        logAction(`✅ Sesión de Instagram iniciada como: ${user.username} (ID: ${user.pk})`);
        botUsername = user.username; // Guardar el nombre de usuario del bot

        // await igClient.simulate.postLoginFlow();

        return true; // Indica éxito

    } catch (error) {
        logAction(`❌ Error crítico configurando Instagram: ${error.message}`);
        if (error.response?.body) {
            logAction(`Detalles del error IG: ${JSON.stringify(error.response.body)}`);
        }
        if (error.message.includes('login_required') || error.message.includes('Login required') || error.response?.body?.message?.includes('checkpoint_required') || error.response?.statusCode === 403 ) {
            logAction("🚨 ¡ERROR DE LOGIN/SESIÓN! Session ID inválido, expirado o se requiere Checkpoint/Verificación.");
            logAction("   Verifica tu SESSION_ID y resuelve cualquier Checkpoint pendiente en la app/web de Instagram.");
        } else {
            logAction("🤔 Posibles causas: Bloqueo de IP (considera proxy), cambio en la API de Instagram, session ID incorrecto.");
        }
        botUsername = null; // Resetear si falla el login
        // Propagar el error para detener el proceso en server.js
        throw new Error(`Fallo en configuración de Instagram: ${error.message}`);
    }
}


// --- Función principal refactorizada ---
async function runBotLogic(config) {
    // Desestructurar configuración
    const {
        instagramSessionId,
        proxy,
        openaiApiKey,
        mongoUri,
        csvPath,
        csvType,
        filterKeywords,
        messageContext,
        maxMessages: configMaxMessages, // Renombrar para evitar conflicto con la variable del bucle
        baseDelaySeconds,
    } = config;

    // Validar configuración esencial faltante (ya se hizo en server.js, pero doble check)
     if (!instagramSessionId || !openaiApiKey || !mongoUri || !csvPath || !messageContext) {
         throw new Error("Configuración incompleta recibida por botLogic.");
     }

    // Inicializar clientes
    openai = new OpenAI({ apiKey: openaiApiKey });

    try {
        // 1. Conectar a MongoDB
        await connectMongo(mongoUri); // Ya maneja errores fatales

        // 2. Configurar Instagram
        // setupInstagram ahora lanza error si falla, deteniendo la ejecución
        await setupInstagram(instagramSessionId, proxy);
        if (!botUsername) { // Doble check por si acaso
            throw new Error("Fallo al obtener el nombre de usuario del bot tras setup de Instagram.");
        }

        // 3. Leer CSV
        let rawAccounts = await readCSV(csvPath); // Lanza error si falla la lectura
        if (!rawAccounts || rawAccounts.length === 0) {
            logAction('❌ El archivo CSV está vacío o no se pudo leer correctamente.');
            return; // Terminar ejecución si el CSV está vacío
        }

        // 4. Filtrar si es tipo 'followers'
        let accountsToProcess = rawAccounts;
        if (csvType === 'followers') {
             // Verificar columnas necesarias para 'followers' ANTES de filtrar
             if (!rawAccounts[0] || !rawAccounts[0].hasOwnProperty('fullName') || !rawAccounts[0].hasOwnProperty('userName')) {
                logAction("❌ Error: El CSV de tipo 'followers' debe contener las columnas 'fullName' y 'userName' (después de la limpieza). Verifica cabeceras detectadas y el archivo.");
                // Podrías lanzar un error aquí si es crítico
                 return; // O simplemente terminar
             }
            accountsToProcess = await filterAccountsByFullName(rawAccounts, filterKeywords);
        }

        if (!accountsToProcess || accountsToProcess.length === 0) {
            logAction('ℹ️ No quedan cuentas para procesar después de la lectura y/o el filtrado.');
            return; // Terminar si no hay cuentas válidas
        }

        // Verificar existencia de userName en las cuentas finales
         if (!accountsToProcess[0].hasOwnProperty('userName')) {
            const potentialUserKeys = ['user', 'usuario']; // Claves alternativas comunes (ya limpias)
            const userKey = Object.keys(accountsToProcess[0]).find(key => potentialUserKeys.includes(key));

            if (userKey) {
                logAction(`⚠️ Columna 'userName' no encontrada directamente, usando '${userKey}' en su lugar.`);
                // Re-mapear para asegurar que todos tengan la propiedad 'userName'
                accountsToProcess = accountsToProcess.map(acc => ({
                     ...acc,
                     userName: acc[userKey] || '' // Asignar userName desde la clave encontrada
                 }));
            } else {
                logAction("❌ Error: No se encontró la columna 'userName' (o similar como 'user', 'usuario') en las cuentas a procesar. Verifica el archivo CSV.");
                return; // Terminar si no hay identificador de usuario
            }
        }

        // Filtrar cuentas que no tengan un userName válido después de todo
        accountsToProcess = accountsToProcess.filter(acc => acc.userName && acc.userName.trim() !== '');
        if (accountsToProcess.length === 0) {
             logAction('ℹ️ No quedan cuentas con un `userName` válido para procesar.');
             return;
         }


        // 5. Determinar cuántos mensajes enviar
        let maxMessagesToSend = configMaxMessages === 0 ? accountsToProcess.length : Math.min(configMaxMessages, accountsToProcess.length);
        logAction(`🎯 Se intentarán enviar mensajes a un máximo de ${maxMessagesToSend} cuentas de ${accountsToProcess.length} disponibles.`);
        logAction(`⏱️ Retardo base entre mensajes: ${baseDelaySeconds} segundos.`);
        logAction(`✉️ Contexto para la IA: "${messageContext}"`);

        // 6. Procesar cuentas
        let sent = 0, failed = 0, skipped = 0;
        const startTime = Date.now();

        for (let i = 0; i < maxMessagesToSend; i++) {
            const account = accountsToProcess[i];
            const targetUsername = account.userName?.trim();

             if (!targetUsername) {
                logAction(`⚠️ Fila ${i + 1} (original): Falta userName o está vacío. Saltando.`);
                skipped++;
                continue; // Saltar esta iteración
            }


            logAction(`\n💬 (${i + 1}/${maxMessagesToSend}) Procesando @${targetUsername}...`);

            // Verificar historial en MongoDB
            try {
                 const existingMessage = await mongoCollection.findOne({ destinatario: targetUsername, accion: 'mensaje_enviado' });
                 if (existingMessage) {
                     logAction(`ℹ️ Ya se envió un mensaje a @${targetUsername} el ${existingMessage.fecha}. Saltando.`);
                     skipped++;
                     if (i < maxMessagesToSend - 1) { // Pequeña pausa antes del siguiente si saltamos
                         await sleep(getRandomDelay(1000, 3000));
                     }
                     continue; // Saltar esta iteración
                 }
             } catch (dbError) {
                 logAction(`⚠️ Error consultando historial en MongoDB para @${targetUsername}: ${dbError.message}. Se continuará igualmente.`);
                 // No es fatal, pero se loguea. Se podría intentar enviar igualmente.
             }


            let igUserData;
            try {
                // Buscar usuario en Instagram
                 try {
                    igUserData = await igClient.user.searchExact(targetUsername);
                } catch (searchError) {
                     if (searchError.message?.includes('User not found')) {
                         logAction(`❓ Usuario @${targetUsername} no encontrado en Instagram.`);
                     } else if (searchError.message?.includes('rate limited')) {
                         logAction(`⏱️ Límite de tasa alcanzado buscando a @${targetUsername}. Esperando 5 minutos...`);
                         await sleep(5 * 60 * 1000);
                         logAction(`⏱️ Reintentando búsqueda para @${targetUsername}...`);
                         try {
                             igUserData = await igClient.user.searchExact(targetUsername);
                         } catch (retryError) {
                             logAction(`❌ Error buscando a @${targetUsername} después de reintento: ${retryError.message}`);
                             // Registrar fallo y continuar al siguiente
                             await mongoCollection.insertOne({ username: targetUsername, error: `Búsqueda fallida tras rate limit: ${retryError.message}`, date: new Date(), status: 'failed_search' });
                             failed++;
                             continue; // Saltar al siguiente usuario
                         }
                     } else if (searchError.message?.includes('checkpoint_required')) {
                         logAction(`🚨 ¡CHECKPOINT REQUERIDO durante búsqueda! Deteniendo script.`);
                         throw new Error("Checkpoint requerido detectado."); // Lanzar error para detener todo
                     } else if (searchError.message?.includes('login_required')) {
                          logAction(`🔒 ¡LOGIN REQUERIDO durante búsqueda! Deteniendo script.`);
                          throw new Error("Login requerido detectado."); // Lanzar error para detener todo
                      }
                       else {
                         logAction(`❌ Error buscando a @${targetUsername}: ${searchError.message}`);
                     }

                     // Si después de manejar errores, igUserData sigue sin definirse, registramos fallo y continuamos
                     if (!igUserData) {
                        await mongoCollection.insertOne({ username: targetUsername, error: `Búsqueda fallida: ${searchError.message || 'Razón desconocida'}`, date: new Date(), status: 'failed_search' });
                        failed++;
                        continue; // Saltar al siguiente usuario
                    }
                 }

                // Si igUserData no tiene 'pk' incluso después de la búsqueda exitosa (raro pero posible)
                 if (!igUserData?.pk) {
                    logAction(`❓ Usuario @${targetUsername} encontrado pero sin ID (pk) válido. Saltando.`);
                    await mongoCollection.insertOne({ username: targetUsername, error: 'Usuario encontrado sin ID (pk) válido', date: new Date(), status: 'failed_search_invalid_data' });
                    failed++;
                    continue; // Saltar al siguiente usuario
                }

                logAction(`👤 Encontrado: ${igUserData.full_name || targetUsername} (ID: ${igUserData.pk})`);

                // Generar mensaje con IA
                const message = await generateAIMessage(targetUsername, igUserData, messageContext);
                 if (!message || message.startsWith("Hola! Te contacto por esto:")) { // Si devuelve el fallback por error o es vacío
                    logAction('⚠️ No se pudo generar mensaje útil con IA o hubo error. Saltando.');
                    failed++;
                    await mongoCollection.insertOne({ username: targetUsername, user_id: igUserData.pk, context: messageContext, error: 'Fallo generación IA o mensaje fallback', date: new Date(), status: 'failed_ai' });
                    continue; // Saltar al siguiente usuario
                }
                logAction(`🤖 Mensaje IA: "${message}"`);

                // Enviar mensaje
                const sendResult = await sendMessage(igUserData.pk, message);

                if (sendResult.shouldStop) {
                    // Si sendMessage indicó que debemos parar (Checkpoint, Login required)
                    logAction("🛑 Deteniendo el proceso debido a un error crítico de Instagram.");
                    throw new Error("Detención solicitada por sendMessage (Checkpoint/Login)");
                 }


                if (sendResult.success) {
                    const currentDate = new Date();
                    const formattedDate = formatDateTime(currentDate);
                    await mongoCollection.insertOne({
                        username: botUsername,
                        accion: 'mensaje_enviado',
                        destinatario: targetUsername,
                        fecha: formattedDate,
                        // message_content: message // Opcional: guardar el mensaje
                    });
                    logAction('✅ Mensaje enviado y registrado en DB.');
                    sent++;
                } else {
                    // El mensaje falló pero no fue un error crítico para detener todo
                    logAction(`❌ Fallo al enviar mensaje a @${targetUsername} (ver logs anteriores).`);
                    failed++;
                    // Registrar fallo de envío si no se hizo ya en sendMessage
                     const recentFailure = await mongoCollection.findOne({
                         destinatario: targetUsername, // Buscar por destinatario ahora
                         status: { $in: ['failed_send', 'failed'] }, // Buscar fallos genéricos o de envío
                         date: { $gt: new Date(Date.now() - 15000) } // Buscar en los últimos 15 seg
                     });
                    if (!recentFailure) {
                         await mongoCollection.insertOne({
                             username: botUsername, // Quién intentó enviar
                             accion: 'envio_fallido',
                             destinatario: targetUsername,
                             user_id: igUserData.pk,
                             message_attempted: message,
                             context: messageContext,
                             error: 'Fallo en el envío (ver logs para detalles)',
                             fecha: formatDateTime(new Date()), // Usar fecha formateada
                             status: 'failed_send', // Status interno para posible reintento futuro
                         });
                     }
                    // No continuar al siguiente inmediatamente, la pausa se manejará después del bloque try/catch
                 }

                 // Pausa inteligente DESPUÉS de procesar un usuario (exitoso o fallido)
                 if (i < maxMessagesToSend - 1) {
                    // Aumentamos significativamente la aleatoriedad para parecer más humano
                    // baseDelaySeconds (ej: 90) + aleatorio entre 30 y 90 segundos
                    const delay = (baseDelaySeconds * 1000) + getRandomDelay(30000, 90000);
                    logAction(`⏳ Esperando ${Math.round(delay / 1000)} segundos antes del próximo usuario...`);
                    await sleep(delay);
                }


            } catch (error) {
                 // Captura errores inesperados DENTRO del bucle para un usuario específico
                 // O errores propagados (como Checkpoint/Login desde search o send)
                 logAction(`💥 Error procesando @${targetUsername}: ${error.message}`);
                 failed++;
                 try {
                      await mongoCollection.insertOne({
                         username: targetUsername,
                         error: `Error inesperado en bucle o error crítico: ${error.message}`,
                         date: new Date(),
                         status: 'failed_unexpected_loop',
                     });
                 } catch (dbError) {
                     logAction(`⚠️ Error adicional al intentar registrar fallo inesperado en DB: ${dbError.message}`);
                 }

                 // Si el error indica detenerse (lanzado por search/send o similar)
                 if (error.message.includes("Checkpoint") || error.message.includes("Login") || error.message.includes("Detención solicitada")) {
                     throw error; // Re-lanzar para que el catch externo lo maneje y termine runBotLogic
                 }

                 // Si fue un error "menor" inesperado, pausa larga y continúa con el siguiente usuario
                 logAction(`⏳ Pausa larga (3-5 min) tras error inesperado con @${targetUsername}...`);
                 await sleep(getRandomDelay(180000, 300000));
                 continue; // Pasar al siguiente usuario en el bucle for
            }
        } // Fin del bucle for

        // 7. Resumen final
        const endTime = Date.now();
        const durationMinutes = Math.round((endTime - startTime) / 60000);
        const durationSeconds = Math.round((endTime - startTime) / 1000);

        logAction(`\n🎉 Proceso completado.`);
        logAction(`⏱️ Duración total: ${durationMinutes} minutos (${durationSeconds} segundos).`);
        logAction(`📊 Total procesados: ${sent + failed + skipped} de ${accountsToProcess.length} (${maxMessagesToSend} intentos).`);
        logAction(`✅ Mensajes enviados con éxito: ${sent}`);
        logAction(`❌ Mensajes fallidos (búsqueda, IA, envío o error): ${failed}`);
        logAction(`⏭️ Usuarios saltados (ya contactados, sin username válido): ${skipped}`);


    } catch (error) {
        // Captura errores fatales FUERA del bucle (conexión DB, setup IG, lectura CSV, error crítico propagado)
        logAction(`💥 Error fatal en la ejecución del bot: ${error.message}`);
        // El error será capturado por el .catch() en server.js también
        throw error; // Re-lanzar para que server.js sepa que falló
    } finally {
        // La desconexión de MongoDB se maneja ahora en el finally de server.js
        // para asegurar que se cierre incluso si hay error temprano.
         if (mongoClient) {
            await mongoClient.close().catch(err => logAction(`⚠️ Error al cerrar conexión MongoDB: ${err.message}`));
            logAction('🔌 Conexión a MongoDB cerrada.');
            mongoClient = null; // Resetear cliente
            mongoCollection = null;
        }
        // No cerramos rl porque ya no existe
        logAction('🏁 Lógica del bot finalizada.');
    }
}

// Exportar la función principal para que server.js la use
module.exports = { runBotLogic };