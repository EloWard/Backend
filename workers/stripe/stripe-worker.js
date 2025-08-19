// Import Stripe library
import Stripe from 'stripe';

// Allowed origins for CORS
const allowedOrigins = ['https://www.eloward.com', 'https://eloward.com'];

// Helper function to generate CORS headers based on the request
function getCorsHeaders(request) {
  const origin = request.headers.get('Origin');
  // Allow specific origins or default to the first allowed origin if the origin matches
  const allowedOrigin = allowedOrigins.includes(origin) ? origin : allowedOrigins[0]; 

  return {
    'Access-Control-Allow-Origin': allowedOrigin,
    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization, Stripe-Signature',
    'Access-Control-Max-Age': '86400', // 24 hours
  };
}

// Helper function to handle errors, ensuring CORS headers are included
function handleError(error, request) {
  console.error('Error:', error);
  const corsHeaders = getCorsHeaders(request);
  
  return new Response(JSON.stringify({ error: error.message || 'An internal error occurred' }), {
    status: error.status || 400, // Use error status if available, default to 400
    headers: { 
      'Content-Type': 'application/json',
      ...corsHeaders, // Spread the CORS headers
    },
  });
}

// Define the worker object
const stripeWorker = {
  async fetch(request, env, ctx) {
    // Special case for webhook endpoint - don't clone the request body since we need the raw body for webhook signature verification
    const url = new URL(request.url);
    if (url.pathname === '/api/webhook' && request.method === 'POST') {
      try {
        // Initialize Stripe with the secret key from environment variables
        const stripe = new Stripe(env.secret_key);
        // Pass the request directly to the webhook handler without cloning it
        return await handleWebhook(request, env, stripe);
      } catch (error) {
        return handleError(error, request);
      }
    }

    // For all other requests, proceed with CORS headers
    const corsHeaders = getCorsHeaders(request);

    // Handle CORS preflight requests
    if (request.method === 'OPTIONS') {
      return new Response(null, {
        status: 204, // No Content
        headers: corsHeaders, // Return CORS headers for preflight
      });
    }

    let response;
    try {
      // Initialize Stripe with the secret key from environment variables
      const stripe = new Stripe(env.secret_key);

      // Route handling for non-webhook routes
      if (url.pathname === '/api/create-checkout-session') {
        response = await handleCreateCheckoutSession(request, env, corsHeaders, stripe);
      } else if (url.pathname === '/api/create-portal-session') {
        response = await handleCreatePortalSession(request, env, corsHeaders, stripe);
      } else {
        response = new Response('Not found', { status: 404 });
      }
    } catch (error) {
      // Pass the original request to handleError
      response = handleError(error, request);
    }

    // Ensure all responses (success, errors, 404s) get CORS headers
    // Clone headers from the generated response to avoid modifying the original
    const finalHeaders = new Headers(response.headers);
    
    // Add CORS headers, potentially overwriting existing ones if needed
    for (const [key, value] of Object.entries(corsHeaders)) {
        finalHeaders.set(key, value);
    }

    // Ensure Content-Type is set for JSON responses if not already present
    if (response.body && typeof response.body === 'object' && !finalHeaders.has('Content-Type')) {
        finalHeaders.set('Content-Type', 'application/json');
    }

    // Return the final response with potentially modified headers
    return new Response(response.body, {
        status: response.status,
        statusText: response.statusText,
        headers: finalHeaders,
    });
  }
};

// Export the worker
export default stripeWorker;

// Handler for creating a checkout session
async function handleCreateCheckoutSession(request, env, corsHeaders, stripe) {
  if (request.method !== 'POST') {
    return new Response('Method not allowed', { status: 405 });
  }

  let data;
  try {
    data = await request.json();
  } catch (err) {
    // Return a specific error for invalid JSON
    return new Response(JSON.stringify({ error: 'Invalid JSON format' }), { 
        status: 400, 
        headers: { 'Content-Type': 'application/json' } // Basic headers, CORS added later
    });
  }
  
  const { channelId, channelName, returnUrl } = data;

  if (!channelId) {
    return new Response(JSON.stringify({ error: 'Channel ID is required' }), {
        status: 400,
        headers: { 'Content-Type': 'application/json' }
    });
  }

  // Ensure we have a channel name, defaulting to channelId if not provided
  const actualChannelName = channelName || channelId;

  try {
    // Verify if the channel exists in the database
    // NOTE: Assumes env.DB is configured correctly for this worker
    // If DB access is needed here, ensure bindings are set up in wrangler.toml
    /* 
    const channel = await env.DB.prepare(
      'SELECT * FROM channels WHERE id = ?'
    ).bind(channelId).first();

    if (!channel) {
      return new Response(JSON.stringify({ error: 'Channel not found' }), {
        status: 404,
        headers: { 'Content-Type': 'application/json' },
      });
    }
    */
    // Temporarily bypass channel check if DB isn't configured for this worker
    console.log(`Received checkout request for channelId: ${channelId}`);

    // Create a checkout session
    const session = await stripe.checkout.sessions.create({
      payment_method_types: ['card'],
      line_items: [
        {
          price: env.price_id, // Ensure price_id is set in env
          quantity: 1,
        },
      ],
      mode: 'subscription',
      success_url: returnUrl || 'https://eloward.com/dashboard?subscription=success',
      cancel_url: returnUrl || 'https://eloward.com/dashboard?subscription=canceled',
      client_reference_id: channelId, // Pass channelId to webhook
      metadata: {
        channelId: channelId, // Numeric Twitch ID
        channelName: actualChannelName, // Twitch username
      },
      allow_promotion_codes: true,
      // Optionally add customer_email if available
    });

    return new Response(JSON.stringify({ url: session.url }), {
      headers: { 'Content-Type': 'application/json' },
    });
  } catch (error) {
    console.error('Checkout session error:', error);
    // Throw the error so it's caught by the main fetch handler's catch block
    // This ensures consistent error handling and CORS headers via handleError
    error.status = 500; // Add a status to the error object
    throw error;
  }
}

// Handler for creating a customer portal session
async function handleCreatePortalSession(request, env, corsHeaders, stripe) {
  if (request.method !== 'POST') {
    return new Response('Method not allowed', { status: 405 });
  }

  let data;
  try {
    data = await request.json();
  } catch (err) {
    return new Response(JSON.stringify({ error: 'Invalid JSON format' }), { 
        status: 400, 
        headers: { 'Content-Type': 'application/json' }
    });
  }
  
  const { channelId, returnUrl } = data;

  if (!channelId) {
    return new Response(JSON.stringify({ error: 'Channel ID is required' }), {
        status: 400,
        headers: { 'Content-Type': 'application/json' }
    });
  }

  try {
    // Get the Stripe customer ID from the database
    const subscribedChannel = await env.DB.prepare(
      'SELECT stripe_customer_id FROM `users` WHERE twitch_id = ?'
    ).bind(channelId).first();

    if (!subscribedChannel || !subscribedChannel.stripe_customer_id) {
      return new Response(JSON.stringify({ error: 'No subscription found for this channel' }), {
        status: 404,
        headers: { 'Content-Type': 'application/json' },
      });
    }
    const customerId = subscribedChannel.stripe_customer_id;
    
    console.log(`Creating portal session for customer: ${customerId}`);

    // Create a portal session
    const session = await stripe.billingPortal.sessions.create({
      customer: customerId, // Use the retrieved customer ID
      return_url: returnUrl || 'https://eloward.com/dashboard',
    });

    return new Response(JSON.stringify({ url: session.url }), {
      headers: { 'Content-Type': 'application/json' },
    });
  } catch (error) {
    console.error('Portal session error:', error);
    error.status = 500;
    throw error; // Re-throw for centralized handling
  }
}

// Handler for Stripe webhooks
async function handleWebhook(request, env, stripe) {
  // Webhooks should not have their body modified or read before signature verification
  if (request.method !== 'POST') {
    return new Response('Method not allowed', { status: 405 });
  }

  const signature = request.headers.get('stripe-signature');
  if (!signature) {
    console.error('No Stripe signature found in the webhook request headers.'); // More specific log
    return new Response('No Stripe signature found in the request headers', { 
      status: 400 
    });
  }

  const webhookSecret = env.WEBHOOK_SECRET;
  if (!webhookSecret) {
    console.error('Webhook secret is not configured in environment variables.');
    return new Response('Webhook secret not configured', { status: 500 });
  }

  // Get the raw request body as text for signature verification
  // This is critical - we must use the exact bytes that Stripe sent
  let payload;
  try {
    payload = await request.text();
  } catch (err) {
    console.error('Failed to read webhook payload:', err);
    return new Response('Failed to read webhook payload', { status: 400 });
  }

  // --- Added Detailed Logging --- 
  console.log(`Received Stripe-Signature: ${signature}`);
  console.log(`Raw payload length: ${payload.length}`);
  // Avoid logging the full secret or sensitive payload data in production logs if possible
  // Consider logging a hash of the payload if needed for comparison: `crypto.subtle.digest('SHA-256', new TextEncoder().encode(payload))`
  // --- End Added Logging ---

  let event;
  try {
    // Use constructEventAsync for async context
    event = await stripe.webhooks.constructEventAsync(
      payload,
      signature,
      webhookSecret
    );
  } catch (err) {
    // Log the specific error from Stripe library
    console.error(`Webhook signature verification failed: ${err.message}`); 
    console.error(`Error details: ${JSON.stringify(err)}`); // Log the full error object if helpful
    return new Response(`Webhook Error: ${err.message}`, { status: 400 });
  }

  // Handle different event types
  try {
    // Log successful webhook receipt
    console.log(`Webhook verified successfully! Event type: ${event.type}, Event ID: ${event.id}`); // Added Event ID
    
    // Create a variable to track which handler was called
    let handlerCalled = null;
    
    switch (event.type) {
      case 'customer.subscription.created':
        await handleSubscriptionCreated(event.data.object, env, stripe);
        handlerCalled = 'subscription.created';
        break;
      case 'customer.subscription.updated':
        await handleSubscriptionUpdated(event.data.object, env, stripe);
        handlerCalled = 'subscription.updated';
        break;
      case 'customer.subscription.deleted':
        await handleSubscriptionDeleted(event.data.object, env, stripe);
        handlerCalled = 'subscription.deleted';
        break;
      case 'invoice.paid':
        // Important: Invoice paid is our primary activation trigger
        console.log(`PROCESSING invoice.paid webhook (Event ID: ${event.id})`);
        await handleInvoicePaid(event.data.object, env, stripe);
        handlerCalled = 'invoice.paid';
        break;
      case 'invoice.payment_failed': 
        // Handle payment failures
        await handleInvoicePaymentFailed(event.data.object, env, stripe);
        handlerCalled = 'invoice.payment_failed';
        break;
      case 'checkout.session.completed':
        await handleCheckoutSessionCompleted(event.data.object, env, stripe);
        handlerCalled = 'checkout.session.completed';
        break;
      default:
        console.log(`Unhandled event type: ${event.type}`);
        handlerCalled = 'none';
    }
    
    console.log(`Webhook handler completed: ${handlerCalled} for event: ${event.id}`);
  } catch (dbError) {
      console.error('Webhook database handler error:', dbError);
      // Return 500 if DB operations fail, so Stripe retries
      return new Response('Webhook handler failed', { status: 500 });
  }

  // Acknowledge receipt of the event with a clear content type
  return new Response(JSON.stringify({ received: true }), {
    headers: { 'Content-Type': 'application/json' }
  });
}

// --- Webhook Helper Functions --- 
// NOTE: These functions now assume env.DB is configured for this worker

// Handler for checkout session completed event (potentially more robust)
async function handleCheckoutSessionCompleted(session, env, stripe) {
    const channelId = session.client_reference_id || session.metadata?.channelId;
    const channelName = session.metadata?.channelName || channelId; // Get the Twitch username, fallback to ID
    const customerId = session.customer;
    const subscriptionId = session.subscription;

    // Add a small delay to reduce race conditions with other webhooks (invoice.paid might come first)
    // This gives invoice.paid a chance to process before we do
    await new Promise(resolve => setTimeout(resolve, 500));

    if (session.mode === 'subscription' && customerId && subscriptionId && channelId && channelName) {
        console.log(`Checkout completed for subscription: ${subscriptionId}, customer: ${customerId}, channel: ${channelName} (ID: ${channelId})`);
        
        let subscriptionEndDate = null;
        let subscriptionStatus = 'incomplete'; // Default status
        try {
            // Retrieve subscription details to get the renewal date and status
            const subscription = await stripe.subscriptions.retrieve(subscriptionId);
            subscriptionStatus = subscription.status;
            
            // Get the renewal date from current_period_end
            if (subscription.current_period_end && typeof subscription.current_period_end === 'number') {
                subscriptionEndDate = new Date(subscription.current_period_end * 1000).toISOString();
                console.log(`Using subscription.current_period_end (${subscription.current_period_end}) as renewal date for subscription ${subscriptionId}`);
            } else {
                // Calculate a default renewal date (1 month from now) for new subscriptions
                const defaultEndDate = new Date();
                defaultEndDate.setMonth(defaultEndDate.getMonth() + 1);
                subscriptionEndDate = defaultEndDate.toISOString();
                console.log(`Subscription ${subscriptionId} has no current_period_end. Using calculated renewal date: ${subscriptionEndDate}`);
            }

            // Ensure metadata is on the subscription object itself for future webhooks
            if ((!subscription.metadata?.channelName || !subscription.metadata?.channelId) && channelName && channelId) {
                try {
                    await stripe.subscriptions.update(subscriptionId, {
                        metadata: {
                            channelId: channelId,
                            channelName: channelName
                        }
                    });
                    console.log(`Updated subscription ${subscriptionId} with metadata: channelId=${channelId}, channelName=${channelName}`);
                } catch (metaUpdateError) {
                    console.error(`Failed to update metadata for subscription ${subscriptionId}:`, metaUpdateError);
                    // Proceed even if metadata update fails, core data is more important
                }
            }
        } catch (subRetrieveError) {
            console.error(`Failed to retrieve subscription ${subscriptionId} during checkout completed handling:`, subRetrieveError);
            // If we can't retrieve the subscription, create a default renewal date
            const defaultEndDate = new Date();
            defaultEndDate.setMonth(defaultEndDate.getMonth() + 1);
            subscriptionEndDate = defaultEndDate.toISOString();
            console.log(`Failed to retrieve subscription. Using calculated renewal date: ${subscriptionEndDate}`);
        }

        // Initially set channel_active=0 and let invoice.paid webhook activate the subscription
        // This ensures we only activate after payment is fully processed
        const isChannelActive = 0; // Always start inactive until payment confirmed via invoice.paid

        // Upsert into the database - but instead of a complex ON CONFLICT query,
        // we'll first check if the record exists by subscription ID, twitch ID, or customer ID
        try {
            // First, try to find the existing record
            const findBySubscriptionQuery = `
                SELECT id, channel_active FROM \`users\` 
                WHERE stripe_subscription_id = ? OR twitch_id = ? OR stripe_customer_id = ?
                LIMIT 1;
            `;
            
            const existingRecord = await env.DB.prepare(findBySubscriptionQuery)
                .bind(subscriptionId, channelId, customerId)
                .first();
            
            if (existingRecord) {
                // Record exists, so update it - but preserve active=1 if already set by invoice.paid
                // This way we won't deactivate a subscription that's already been activated
                
                // First, check if it's already active so we don't overwrite it
                const currentActive = existingRecord.channel_active || 0;
                const finalActive = currentActive === 1 ? 1 : isChannelActive; // Keep channel_active=1 if already active
                
                const updateQuery = `
                    UPDATE \`users\` SET
                        channel_name = ?,
                        twitch_id = COALESCE(?, twitch_id),
                        stripe_customer_id = ?,
                        stripe_subscription_id = ?,
                                            channel_active = ?,
                    subscription_end_date = COALESCE(?, subscription_end_date),
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
            `;
            
            await env.DB.prepare(updateQuery)
                .bind(channelName.toLowerCase(), channelId, customerId, subscriptionId, finalActive, subscriptionEndDate, existingRecord.id)
                .run();
            
            const statusMessage = finalActive === 1 ? 
                "Preserved existing active status" : 
                "Set inactive status (waiting for invoice.paid)";
                
            console.log(`DB: Updated existing subscription record for ${channelName} (ID: ${channelId}), SubID: ${subscriptionId}. ${statusMessage}`);
        } else {
            // No existing record, insert a new one with channel_active=0
            const insertQuery = `
                INSERT INTO \`users\` 
                    (channel_name, twitch_id, stripe_customer_id, stripe_subscription_id, subscription_end_date, channel_active, db_reads, successful_lookups)
                VALUES (?, ?, ?, ?, ?, ?, 0, 0)
            `;
            
            await env.DB.prepare(insertQuery)
                .bind(channelName.toLowerCase(), channelId, customerId, subscriptionId, subscriptionEndDate, isChannelActive)
                    .run();
                
                console.log(`DB: Created new subscription record for ${channelName} (ID: ${channelId}), SubID: ${subscriptionId}. Will be activated when payment is confirmed via invoice.paid.`);
            }

        } catch (dbError) {
            console.error(`Database error during checkout completed for channel ${channelName} (SubID: ${subscriptionId}):`, dbError);
            throw dbError; // Re-throw to trigger webhook retry
        }
    } else {
        console.warn('Checkout session completed, but missing critical data (mode, customerId, subscriptionId, channelId/Name). Session:', session.id);
    }
}

// Handler for subscription created event (REDUCED RELIANCE - checkout.session.completed is preferred)
async function handleSubscriptionCreated(subscription, env, stripe) {
  const customerId = subscription.customer;
  const subscriptionId = subscription.id;
  // Attempt to get identifiers from metadata, but don't fail if missing
  const channelId = subscription.metadata?.channelId;
  const channelName = subscription.metadata?.channelName;

  console.log(`Subscription created event received: ${subscriptionId}, customer: ${customerId}, channel (from meta): ${channelName} (ID: ${channelId})`);
  
  // We primarily rely on checkout.session.completed now. 
  // This handler might only be useful as a fallback or for logging.
  // We won't perform DB updates here unless absolutely necessary, 
  // as checkout.session.completed should handle the initial record creation/update.

  if (!customerId || !subscriptionId) {
      console.error('Subscription created event missing customer or subscription ID. Cannot process further.');
      return; 
  }

  // Optional: Could add logic here to update the DB *if* a record with this subscriptionId 
  // already exists but is missing customerId or channel info, but it adds complexity.
  // For now, we assume checkout.session.completed handles the main logic.
  console.log(`Subscription created event for ${subscriptionId} processed (no DB action taken by default).`);

  /* // Example of potential fallback DB logic (use with caution):
  try {
    let subscriptionEndDate = null;
    if (subscription.current_period_end && typeof subscription.current_period_end === 'number') {
        subscriptionEndDate = new Date(subscription.current_period_end * 1000).toISOString();
    } else {
        console.warn(`Subscription ${subscriptionId} (created event) has invalid current_period_end.`);
    }
    const isActive = ['active', 'trialing'].includes(subscription.status) ? 1 : 0;

    // Attempt to update ONLY if the subscription ID exists
    const updateQuery = `
        UPDATE \`subscribed-channels\` SET
            stripe_customer_id = COALESCE(?, stripe_customer_id),
            channel_name = COALESCE(?, channel_name),
            twitch_id = COALESCE(?, twitch_id),
            subscription_end_date = COALESCE(?, subscription_end_date),
            active = ?, 
            updated_at = CURRENT_TIMESTAMP
        WHERE stripe_subscription_id = ?;
    `;
    const result = await env.DB.prepare(updateQuery)
        .bind(customerId, channelName, channelId, subscriptionEndDate, isActive, subscriptionId)
        .run();
       
    if (result.meta.changes > 0) {
        console.log(`DB: Updated existing record for SubID ${subscriptionId} via subscription.created event.`);
    } else {
        console.log(`DB: No existing record found for SubID ${subscriptionId} during subscription.created event (expected, handled by checkout.session.completed).`);
    }
  } catch (dbError) {
      console.error(`Database error during subscription.created fallback for SubID ${subscriptionId}:`, dbError);
      // Don't throw here, as it's a fallback
  }
  */
}

// Handler for subscription updated event
async function handleSubscriptionUpdated(subscription, env, stripe) {
  const subscriptionId = subscription.id;
  const customerId = subscription.customer;
  const status = subscription.status;
  
  // Only deactivate (active=0) for canceled/past_due/unpaid subscriptions
  const shouldDeactivate = ['canceled', 'past_due', 'unpaid', 'incomplete_expired'].includes(status);
  
  // Activate (active=1) if Stripe status is 'active'
  // Note: 'trialing' status from Stripe would also normally be considered active in our system,
  // but we're not using trials so we only need to check for 'active'
  const shouldActivate = status === 'active' && !shouldDeactivate;
  
  let subscriptionEndDate = null;
  
  // Get the renewal date from current_period_end
  if (subscription.current_period_end && typeof subscription.current_period_end === 'number') {
    subscriptionEndDate = new Date(subscription.current_period_end * 1000).toISOString();
    console.log(`Using subscription.current_period_end (${subscription.current_period_end}) as renewal date for subscription ${subscriptionId} in updated event`);
  } else {
    // Calculate a default renewal date for subscriptions missing current_period_end
    const defaultEndDate = new Date();
    defaultEndDate.setMonth(defaultEndDate.getMonth() + 1);
    subscriptionEndDate = defaultEndDate.toISOString();
    console.log(`Subscription ${subscriptionId} (updated event) has no current_period_end. Using calculated renewal date: ${subscriptionEndDate}`);
  }

  // Attempt to get channel identifiers from metadata for logging/completeness
  const channelId = subscription.metadata?.channelId;
  const channelName = subscription.metadata?.channelName;

  console.log(`Subscription updated: ${subscriptionId}, Cust: ${customerId}, Chan: ${channelName}(${channelId}), Status: ${status}, Renewal Date: ${subscriptionEndDate || 'N/A'}, Set Active: ${shouldActivate ? 1 : (shouldDeactivate ? 0 : 'unchanged')}`);

  if (!subscriptionId) {
      console.error('Subscription updated event missing subscription ID. Cannot process.');
      return;
  }
  
  // Update based on stripe_subscription_id
  try {
    // First check if the record exists
    const findQuery = `
        SELECT id, channel_active FROM \`users\` 
        WHERE stripe_subscription_id = ?
        LIMIT 1
    `;
    
    const existingRecord = await env.DB.prepare(findQuery)
      .bind(subscriptionId)
      .first();
      
    if (existingRecord) {
      // The update query depends on whether we're changing active status
      let updateQuery;
      let bindings;
      
      if (shouldDeactivate) {
        // If we're deactivating the subscription
        updateQuery = `
            UPDATE \`users\` SET 
                channel_active = 0, 
                stripe_customer_id = ?,
                channel_name = COALESCE(?, channel_name),
                twitch_id = COALESCE(?, twitch_id),
                subscription_end_date = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        `;
        bindings = [customerId, channelName, channelId, subscriptionEndDate, existingRecord.id];
        
        await env.DB.prepare(updateQuery)
          .bind(...bindings)
          .run();
          
        console.log(`DB: Set subscription ${subscriptionId} to channel_active=0 due to status: ${status}`);
      } else if (shouldActivate && existingRecord.channel_active !== 1) {
        // If status is 'active' and record isn't already active, set channel_active=1
        updateQuery = `
            UPDATE \`users\` SET 
                channel_active = 1,
                stripe_customer_id = ?,
                channel_name = COALESCE(?, channel_name),
                twitch_id = COALESCE(?, twitch_id),
                subscription_end_date = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        `;
        bindings = [customerId, channelName, channelId, subscriptionEndDate, existingRecord.id];
        
        await env.DB.prepare(updateQuery)
          .bind(...bindings)
          .run();
          
        console.log(`DB: Set subscription ${subscriptionId} to channel_active=1 from subscription.updated because status is ${status}`);
      } else {
        // Otherwise, update info but don't change active status
        updateQuery = `
            UPDATE \`subscribed-channels\` SET 
                stripe_customer_id = ?,
                channel_name = COALESCE(?, channel_name),
                twitch_id = COALESCE(?, twitch_id),
                subscription_end_date = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        `;
        bindings = [customerId, channelName, channelId, subscriptionEndDate, existingRecord.id];
        
        await env.DB.prepare(updateQuery)
          .bind(...bindings)
          .run();
          
        console.log(`DB: Updated subscription ${subscriptionId} info without changing active status (current channel_active=${existingRecord.channel_active})`);
      }
      
              // Add verification step for subscription active status
        const verifyQuery = `
            SELECT channel_active FROM \`users\` 
            WHERE stripe_subscription_id = ?
            LIMIT 1
        `;
      
      const verifyResult = await env.DB.prepare(verifyQuery)
          .bind(subscriptionId)
          .first();
          
      if (verifyResult) {
          console.log(`VERIFICATION: After subscription.updated, subscription ${subscriptionId} channel_active status is: ${verifyResult.channel_active}`);
      }
    } else {
      console.warn(`DB: Subscription ${subscriptionId} not found in DB for subscription.updated event. It might not have been fully processed by checkout.session.completed yet.`);
      
      // Only create a new record if we have required data
      if (channelName) {
        const insertQuery = `
            INSERT INTO \`users\` 
                (channel_name, twitch_id, stripe_customer_id, stripe_subscription_id, subscription_end_date, channel_active, db_reads, successful_lookups)
            VALUES (?, ?, ?, ?, ?, ?, 0, 0)
        `;
        
        // Set channel_active=1 directly if Stripe status is 'active'
        const newChannelActive = shouldActivate ? 1 : 0;
        
        await env.DB.prepare(insertQuery)
          .bind(channelName.toLowerCase(), channelId, customerId, subscriptionId, subscriptionEndDate, newChannelActive)
          .run();
          
        if (shouldActivate) {
          console.log(`DB: Created new subscription record with channel_active=1 from subscription.updated because status is ${status}`);
        } else {
          console.log(`DB: Created new subscription record with channel_active=0 from subscription.updated. Will be activated by invoice.paid.`);
        }
      }
    }
  } catch (dbError) {
    console.error(`Database error during subscription.updated for SubID ${subscriptionId}:`, dbError);
    throw dbError; // Re-throw to trigger webhook retry
  }
}

// Handler for subscription deleted event
async function handleSubscriptionDeleted(subscription, env, stripe) {
  const subscriptionId = subscription.id;
  const customerId = subscription.customer;
  // Metadata might be missing here, don't rely on it
  const channelId = subscription.metadata?.channelId;
  const channelName = subscription.metadata?.channelName;
  
  console.log(`Subscription deleted event: ${subscriptionId}, Customer: ${customerId}, Channel (from meta, might be missing): ${channelName} (${channelId})`);

  if (!subscriptionId) {
    console.error('Subscription deleted event missing subscription ID. Cannot process.');
    return; // Cannot proceed without subscriptionId
  }

  // Mark subscription as inactive in database based on stripe_subscription_id
  try {
    const query = `
        UPDATE \`users\` 
        SET channel_active = 0, updated_at = CURRENT_TIMESTAMP 
        WHERE stripe_subscription_id = ?;
    `;
    const result = await env.DB.prepare(query)
      .bind(subscriptionId)
      .run();
      
    if (result.meta.changes === 0) {
      console.warn(`DB: Subscription ${subscriptionId} not found in DB for subscription.deleted event.`);
    } else {
      console.log(`DB: Marked subscription ${subscriptionId} inactive from subscription.deleted`);
    }
  } catch (dbError) {
    console.error(`Database error during subscription.deleted for SubID ${subscriptionId}:`, dbError);
    throw dbError; // Re-throw to trigger webhook retry
  }
}

// Handler for invoice paid event (updates end date and activates subscription)
async function handleInvoicePaid(invoice, env, stripe) {
  const subscriptionId = invoice.subscription;
  const customerId = invoice.customer;

  if (!subscriptionId || !customerId) {
      console.log(`Invoice paid event ${invoice.id} is missing subscription ID (${subscriptionId}) or customer ID (${customerId}). Ignoring.`);
      return; // Ignore invoices not linked to a subscription/customer we can identify
  }
  
  // Initialize subscription end date (renewal date)
  let subscriptionEndDate = null;
  
  // Retrieve the subscription to get renewal date, status and metadata
  try {
    const subscription = await stripe.subscriptions.retrieve(subscriptionId);
    const status = subscription.status;
    
    // Always set channel_active=1 when invoice.paid is received
    // This is the definitive signal that payment has been processed successfully
    const isChannelActive = 1; // Invoice paid = active subscription
    
    // Get the actual renewal date from current_period_end
    if (subscription.current_period_end && typeof subscription.current_period_end === 'number') {
        subscriptionEndDate = new Date(subscription.current_period_end * 1000).toISOString();
        console.log(`Using subscription.current_period_end (${subscription.current_period_end}) as renewal date for subscription ${subscriptionId}`);
    } else {
        // Calculate a default renewal date (1 month from now) for subscriptions
        const defaultEndDate = new Date();
        defaultEndDate.setMonth(defaultEndDate.getMonth() + 1);
        subscriptionEndDate = defaultEndDate.toISOString();
        console.log(`Subscription ${subscriptionId} (invoice.paid event) has no current_period_end. Using calculated renewal date: ${subscriptionEndDate}`);
    }

    // Get identifiers from metadata (hopefully populated by checkout.session.completed)
    const channelId = subscription.metadata?.channelId;
    const channelName = subscription.metadata?.channelName;

    console.log(`ACTIVATION: Invoice paid for subscription: ${subscriptionId}, Cust: ${customerId}, Chan: ${channelName}(${channelId}), Status: ${status}, Renewal Date: ${subscriptionEndDate || 'N/A'}`);

    // --- Database Update --- 
    try {
        // First check if the record exists by subscription ID OR customer ID OR channel name
        // This increases our chance of finding the record if it was created by a different webhook
        const findQuery = `
            SELECT id FROM \`users\` 
            WHERE stripe_subscription_id = ? 
               OR (stripe_customer_id = ? AND stripe_customer_id IS NOT NULL)
               OR (channel_name = ? AND channel_name IS NOT NULL)
            LIMIT 1
        `;
        
        const existingRecord = await env.DB.prepare(findQuery)
            .bind(subscriptionId, customerId, channelName?.toLowerCase())
            .first();
            
        if (existingRecord) {
            // Update existing record - ALWAYS SET CHANNEL_ACTIVE=1
            const updateQuery = `
                UPDATE \`users\` SET 
                    channel_active = 1, -- Always activate on invoice.paid
                    stripe_customer_id = ?,
                    stripe_subscription_id = ?,
                    channel_name = COALESCE(?, channel_name),
                    twitch_id = COALESCE(?, twitch_id),
                    subscription_end_date = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
            `;
            
            await env.DB.prepare(updateQuery)
                .bind(customerId, subscriptionId, channelName, channelId, subscriptionEndDate, existingRecord.id)
                .run();
                
            console.log(`DB: ACTIVATED subscription ${subscriptionId} (channel_active=1) from invoice.paid`);
        } else if (channelName) {
            // Insert new record if channelName is available
            const insertQuery = `
                INSERT INTO \`users\` 
                    (channel_name, twitch_id, stripe_customer_id, stripe_subscription_id, subscription_end_date, channel_active, db_reads, successful_lookups)
                VALUES (?, ?, ?, ?, ?, 1, 0, 0) -- Set channel_active=1 directly
            `;
            
            await env.DB.prepare(insertQuery)
                .bind(channelName.toLowerCase(), channelId, customerId, subscriptionId, subscriptionEndDate)
                .run();
                
            console.log(`DB: Created and ACTIVATED new record (channel_active=1) for ${channelName} (SubID: ${subscriptionId}) from invoice.paid`);
        } else {
            console.error(`DB: Cannot insert record for SubID ${subscriptionId} from invoice.paid because channelName is missing from subscription metadata.`);
        }
        
        // Double-check activation status to ensure it's set properly
        const verifyQuery = `
            SELECT channel_active FROM \`users\` 
            WHERE stripe_subscription_id = ?
            LIMIT 1
        `;
        
        const verifyResult = await env.DB.prepare(verifyQuery)
            .bind(subscriptionId)
            .first();
            
        if (verifyResult) {
            console.log(`VERIFICATION: Subscription ${subscriptionId} channel_active status is now: ${verifyResult.channel_active}`);
            if (verifyResult.channel_active !== 1) {
                console.error(`CRITICAL: Subscription ${subscriptionId} should be channel_active=1 but is ${verifyResult.channel_active}!`);
                // Force update as a last resort
                const forceActivateQuery = `
                    UPDATE \`users\` SET 
                        channel_active = 1,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE stripe_subscription_id = ?
                `;
                
                await env.DB.prepare(forceActivateQuery)
                    .bind(subscriptionId)
                    .run();
                    
                console.log(`RECOVERY: Forced activation of subscription ${subscriptionId} to channel_active=1`);
            }
        }
    } catch(dbError) {
        console.error(`Database error during invoice.paid for SubID ${subscriptionId}:`, dbError);
        throw dbError; // Re-throw to trigger retry
    }
  } catch (error) {
      console.error(`Error retrieving subscription ${subscriptionId} during invoice.paid handling:`, error);
      
      // Even if subscription retrieval fails, we can still calculate a default renewal date
      // and activate the subscription since payment was confirmed
      const defaultEndDate = new Date();
      defaultEndDate.setMonth(defaultEndDate.getMonth() + 1);
      subscriptionEndDate = defaultEndDate.toISOString();
      console.log(`Failed to retrieve subscription. Using calculated renewal date: ${subscriptionEndDate} for database update`);
      
      try {
          // Try updating any record with this subscription ID to channel_active=1
          const updateQuery = `
              UPDATE \`users\` SET 
                  subscription_end_date = ?,
                  channel_active = 1,
                  updated_at = CURRENT_TIMESTAMP
              WHERE stripe_subscription_id = ?
          `;
          const result = await env.DB.prepare(updateQuery)
              .bind(subscriptionEndDate, subscriptionId)
              .run();
              
          if (result.meta.changes > 0) {
              console.log(`DB: ACTIVATED subscription ${subscriptionId} (channel_active=1) using calculated renewal date`);
              return; // Success, exit function
          } else {
              console.warn(`No record found with subscription ID ${subscriptionId} for fallback activation`);
          }
      } catch (fallbackDbError) {
          console.error(`Fallback DB update failed for subscription ${subscriptionId}:`, fallbackDbError);
      }
      
      if (error.type === 'StripeInvalidRequestError') {
          console.warn(`Subscription ${subscriptionId} not found via Stripe API during invoice.paid. It might have been deleted.`);
          return; // Subscription likely deleted, ignore
      }
      throw error; // Re-throw other errors
  }
}

// Handler for invoice payment failed event (marks subscription as inactive)
async function handleInvoicePaymentFailed(invoice, env, stripe) {
  const subscriptionId = invoice.subscription;
  const customerId = invoice.customer;
  
  if (!subscriptionId || !customerId) {
    console.log(`Invoice payment failed event ${invoice.id} missing subscription ID (${subscriptionId}) or customer ID (${customerId}). Ignoring.`);
    return; 
  }
  
  try {
    // Retrieve the subscription to get the current details and metadata
    const subscription = await stripe.subscriptions.retrieve(subscriptionId);
    const status = subscription.status;
    const channelId = subscription.metadata?.channelId;
    const channelName = subscription.metadata?.channelName;
    
    console.log(`Invoice payment failed for subscription: ${subscriptionId}, Cust: ${customerId}, Chan: ${channelName}(${channelId}), Status: ${status}`);
    
    // If status indicates payment issue, mark as inactive in our database
    if (['past_due', 'unpaid', 'incomplete_expired', 'canceled'].includes(status)) { // Added 'canceled' for robustness
      try {
        const query = `
            UPDATE \`users\` 
            SET channel_active = 0, updated_at = CURRENT_TIMESTAMP 
            WHERE stripe_subscription_id = ?;
        `;
        const result = await env.DB.prepare(query)
          .bind(subscriptionId)
          .run(); 
        
        if (result.meta.changes === 0) {
          console.warn(`DB: Subscription ${subscriptionId} not found in DB for invoice.payment_failed update.`);
        } else {
          console.log(`DB: Marked subscription ${subscriptionId} inactive due to payment failure/status (${status})`);
        }
      } catch (dbError) {
        console.error(`Database error during invoice.payment_failed for SubID ${subscriptionId}:`, dbError);
        throw dbError;
      }
    } else {
      console.log(`Subscription ${subscriptionId} status is ${status}. No database action taken for invoice.payment_failed.`);
    }
  } catch (error) {
    console.error(`Error retrieving subscription ${subscriptionId} during invoice.payment_failed handling:`, error);
      if (error.type === 'StripeInvalidRequestError') {
          console.warn(`Subscription ${subscriptionId} not found via Stripe API during invoice.payment_failed. It might have been deleted.`);
          return; // Subscription likely deleted, ignore
      }
      throw error; // Re-throw other errors
  }
}
