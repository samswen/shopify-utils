'use strict';

const axios = require('axios');
const fs = require('fs');
const os = require('os');
const { spawn } = require('child_process');
const { stdout, stderr } = require('process');

module.exports = {
    setup,
    get_myshop_info,
    get_locations,
    get_product,
    get_collection,
    find_product,
    subscribe_webhook_event,
    delete_webhook,
    get_all_webhooks,
    get_products_count,
    get_next_page_products,
    get_next_page_collects,
    get_next_page_smart_collections,
    get_next_page_custom_collections,
    get_collection_metafields,
    get_all_products,
    get_smart_collections_count,
    get_all_smart_collections,
    get_custom_collections_count,
    get_all_custom_collections,
    get_metafields,
    get_all_collects,
    delete_all_products,
    delete_product,
    post_product,
    put_product,
    update_inventory_item,
    update_product_metafields,
    update_variant_metafields,
    get_product_metafields,
    post_product_metafields,
    get_variant_metafields,
    post_variant_metafields,
    delete_all_metafields,
    update_product_tags,
    update_product_body_html,
    delete_metafield,
    get_images,
    get_all_media_graphql,
    upload_video,
    initialize_media_order,
    reorder_media,
    upload_videos,
    bulk_update_inventory_items,
    bulk_update_products,
    set_inventory_levels,
    delist_product,
    delist_inventory_item,
    get_themes,
    get_theme,
    post_theme,
    put_theme,
    delete_theme,
    get_assets,
    get_asset,
    put_asset,
    delete_asset,
    get_script_tags,
    get_script_tags_count,
    get_script_tag,
    post_script_tag,
    put_script_tag,
    delete_script_tag,
};

let logger = console;
let http_request_timeout = 180000;
let main_location_id = null;
let aws_events_arn = null;
let shopify_api_version = '2020-07';

function setup(config, arg_logger) {
    if (config) {
        main_location_id = config.get('main_location_id');
        aws_events_arn = config.get('aws_events_arn');
        const request_timeout = config.get('http_request_timeout');
        if (request_timeout) {
            http_request_timeout = request_timeout;
        }
        const api_version = config.get('shopify_api_version');
        if (api_version) {
            shopify_api_version = api_version;
        }
    }
    if (arg_logger) {
        logger = arg_logger;
    }
}

async function get_myshop_info(client) {
    try {
        const options = get_axios_options(client, 'get', `/admin/api/${shopify_api_version}/shop.json`);
        const response = await axios_multi_tries(options);
        return response.data;
    } catch(err) {
        logger.error(err.message);
        return false;
    }
}

async function get_script_tags_count(client) {
    try {
        const options = get_axios_options(client, 'get', `/admin/api/${shopify_api_version}/script_tags/count.json`);
        const response = await axios_multi_tries(options);
        return response.data;
    } catch(err) {
        logger.error(err.message);
        return false;
    }
}

async function get_script_tag(client, tag_id) {
    try {
        const options = get_axios_options(client, 'get', `/admin/api/${shopify_api_version}/script_tags/${tag_id}.json`);
        const response = await axios_multi_tries(options);
        return response.data;
    } catch(err) {
        logger.error(err.message);
        return false;
    }
}

async function delete_script_tag(client, tag_id) {
    try {
        const options = get_axios_options(client, 'delete', `/admin/api/${shopify_api_version}/script_tags/${tag_id}.json`);
        const response = await axios_multi_tries(options);
        return response.data;
    } catch(err) {
        logger.error(err.message);
        return false;
    }
}

async function put_script_tag(client, tag_id, data) {
    try {
        const options = get_axios_options(client, 'put', `/admin/api/${shopify_api_version}/script_tags/${tag_id}.json`, null, data);
        const response = await axios_multi_tries(options);
        return response.data;
    } catch(err) {
        logger.error(err.message);
        return false;
    }
}

async function post_script_tag(client, data) {
    try {
        const options = get_axios_options(client, 'post', `/admin/api/${shopify_api_version}/script_tags.json`, null, data);
        const response = await axios_multi_tries(options);
        return response.data;
    } catch(err) {
        logger.error(err.message);
        return false;
    }
}

async function get_script_tags(client) {
    try {
        const options = get_axios_options(client, 'get', `/admin/api/${shopify_api_version}/script_tags.json`);
        const response = await axios_multi_tries(options);
        return response.data;
    } catch(err) {
        logger.error(err.message);
        return false;
    }
}

async function get_asset(client, theme_id, filepath) {
    try {
        const options = get_axios_options(client, 'get', `/admin/api/${shopify_api_version}/themes/${theme_id}/assets.json?asset[key]=${filepath}`);
        const response = await axios_multi_tries(options);
        return response.data;
    } catch(err) {
        logger.error(err.message);
        return false;
    }
}

async function put_asset(client, theme_id, data) {
    try {
        const options = get_axios_options(client, 'put', `/admin/api/${shopify_api_version}/themes/${theme_id}/assets.json`, null, data);
        const response = await axios_multi_tries(options);
        return response.data;
    } catch(err) {
        logger.error(err.message);
        return false;
    }
}

async function delete_asset(client, theme_id, filepath) {
    try {
        const options = get_axios_options(client, 'delete', `/admin/api/${shopify_api_version}/themes/${theme_id}/assets.json?asset[key]=${filepath}`);
        const response = await axios_multi_tries(options);
        return response.data;
    } catch(err) {
        logger.error(err.message);
        return false;
    }
}

async function get_assets(client, theme_id) {
    try {
        const options = get_axios_options(client, 'get', `/admin/api/${shopify_api_version}/themes/${theme_id}/assets.json`);
        const response = await axios_multi_tries(options);
        return response.data;
    } catch(err) {
        logger.error(err.message);
        return false;
    }
}

async function get_themes(client) {
    try {
        const options = get_axios_options(client, 'get', `/admin/api/${shopify_api_version}/themes.json`);
        const response = await axios_multi_tries(options);
        return response.data;
    } catch(err) {
        logger.error(err.message);
        return false;
    }
}

async function get_theme(client, theme_id) {
    try {
        const options = get_axios_options(client, 'get', `/admin/api/${shopify_api_version}/themes/${theme_id}.json`);
        const response = await axios_multi_tries(options);
        return response.data;
    } catch(err) {
        logger.error(err.message);
        return false;
    }
}

async function post_theme(client, data) {
    try {
        const options = get_axios_options(client, 'post', `/admin/api/${shopify_api_version}/themes.json`, null, data);
        const response = await axios_multi_tries(options);
        return response.data;
    } catch(err) {
        logger.error(err.message);
        return false;
    }
}

async function put_theme(client, theme_id, data) {
    try {
        const options = get_axios_options(client, 'put', `/admin/api/${shopify_api_version}/themes/${theme_id}.json`, null, data);
        const response = await axios_multi_tries(options);
        return response.data;
    } catch(err) {
        logger.error(err.message);
        return false;
    }
}

async function delete_theme(client, theme_id) {
    try {
        const options = get_axios_options(client, 'delete', `/admin/api/${shopify_api_version}/themes/${theme_id}.json`);
        const response = await axios_multi_tries(options);
        return response.data;
    } catch(err) {
        logger.error(err.message);
        return false;
    }
}

async function get_locations(client) {
    try {
        const options = get_axios_options(client, 'get', `/admin/api/${shopify_api_version}/locations.json`);
        const response = await axios_multi_tries(options);
        return response.data;
    } catch(err) {
        logger.error(err.message);
        return false;
    }
}

async function set_inventory_levels(client, inventory_item_id, available, location_id = null) {
    try {
        if (!location_id && main_location_id) {
            location_id = main_location_id;
        }
        const data = { location_id, inventory_item_id, available};
        const options = get_axios_options(client, 'post', `/admin/api/${shopify_api_version}/inventory_levels/set.json`, null, data);
        const response = await axios_multi_tries(options);
        return response.data;
    } catch(err) {
        logger.error(err.message);
        return false;
    }
}

async function delist_product(client, product) {
    if (typeof product === 'number') {
        const product_id = product;
        product = await get_product(client, product_id);
    }
    for (const variant of product.variants) {
        await set_inventory_levels(client, variant.inventory_item_id, 0);
    }
}

async function delist_inventory_item(client, inventory_item_id) {
    await set_inventory_levels(client, inventory_item_id, 0);
}

async function upload_videos(client, product_id, videos) {
    let not_ready_count = 0;
    const graphql_api_id = 'gid://shopify/Product/' + product_id;
    const result = await get_all_media_graphql(client, graphql_api_id);
    if (!result || !result.data || !result.data.product || !result.data.product || !result.data.product.media ||
        !result.data.product.media.edges || !Array.isArray(result.data.product.media.edges) ||
        result.data.product.media.edges.length === 0) {
        logger.debug(JSON.stringify(result, null, 2));
        logger.error('unexpected result, expecting result.data.product.media.edges to be an array');
        return false;
    }
    const edges = result.data.product.media.edges;
    for (const edge of edges) {
        const node = edge.node;
        if (node.mediaContentType === 'VIDEO') {
            if (node.status !== 'FAILED') {
                const key = node.alt;
                if (videos[key]) {
                    delete videos[key];
                }
            }
            if (node.status !== 'READY') {
                logger.log('====== status not ready (' + node.status + ') for ' + product_id + '/' + node.alt);
                not_ready_count++;
            }
        }
    }
    if (Object.keys(videos).length === 0) {
        logger.info('*** Nothing to do! videos are already uploaded for ' + product_id);
        return not_ready_count;
    }
    not_ready_count = 0;
    for (const alt in videos) {
        not_ready_count++;
        const source_url = videos[alt];
        const media_id = await upload_video(client, product_id, source_url, alt);
        if (!media_id) {
            logger.error('failed to upload video for ' + product_id + '/' + alt);
        }
    }
    return not_ready_count;
}

async function initialize_media_order(client, product_id, type) {
    if (!['settings', 'wedding-rings', 'jewelry'].includes(type)) {
        logger.error('unsupported type: ' + type);
        return false;
    }
    const graphql_api_id = 'gid://shopify/Product/' + product_id;
    const result = await get_all_media_graphql(client, graphql_api_id);
    //logger.info(JSON.stringify(result, null, 2));
    if (!result || !result.data || !result.data.product || !result.data.product || !result.data.product.media ||
        !result.data.product.media.edges || !Array.isArray(result.data.product.media.edges) ||
        result.data.product.media.edges.length === 0) {
        logger.error('unexpected result, expecting result.data.product.media.edges to be an array');
        return false;
    }
    const images = {};
    const edges = result.data.product.media.edges;
    const key_images_order = [];
    let need_reorder_images = false;
    let first_video_key = null;
    for (const edge of edges) {
        const node = edge.node;
        if (node.status !== 'READY') {
            logger.error('there is media not in READY status yet for ' + product_id + ' status: ' + node.status);
            logger.trace(JSON.stringify(node, null, 2));
            return false;
        }
        if (node.mediaContentType === 'IMAGE') {
            if (!need_reorder_images && first_video_key) {
                need_reorder_images = true;
            }
            images[node.alt] = {id: node.id};
            key_images_order.push(node.alt);
        } else if (node.mediaContentType === 'VIDEO') {
            if (!first_video_key) {
                first_video_key = node.alt;
            }
        }
    }
    if (!first_video_key) {
        logger.info('there is no video to reorder for ' + product_id);
        return true;
    }
    if (!need_reorder_images) {
        logger.info('there is no need to initialize media order for ' + product_id);
        return true;
    }
    const moves = [];
    for (let i = key_images_order.length - 1; i >= 0; i--) {
        const key = key_images_order[i];
        const image = images[key];
        moves.push({id: image.id, newPosition: 0});
    }
    const move_result = await reorder_media_graphql(client, graphql_api_id, moves);
    if (move_result && move_result.data && move_result.data.productReorderMedia && move_result.data.productReorderMedia.mediaUserErrors &&
        Array.isArray(move_result.data.productReorderMedia.mediaUserErrors) && move_result.data.productReorderMedia.mediaUserErrors.length === 0) {
        return true;
    } else {
        logger.info('move_result', JSON.stringify(move_result, null, 2));
        logger.error('unexpected move_result, expecting move_result.data.productReorderMedia.mediaUserErrors is empty for ' + product_id);
        return false;
    }
}

async function reorder_media(client, product_id, type) {
    if (!['settings', 'wedding-rings', 'jewelry'].includes(type)) {
        logger.error('unsupported type: ' + type);
        return false;
    }
    const graphql_api_id = 'gid://shopify/Product/' + product_id;
    const result = await get_all_media_graphql(client, graphql_api_id);
    if (!result || !result.data || !result.data.product || !result.data.product || !result.data.product.media ||
        !result.data.product.media.edges || !Array.isArray(result.data.product.media.edges) ||
        result.data.product.media.edges.length === 0) {
        logger.error('unexpected result, expecting result.data.product.media.edges to be an array');
        return false;
    }
    const images = {};
    const videos = {};
    const edges = result.data.product.media.edges;
    const key_images_order = [];
    let first_video_key = null;
    for (const edge of edges) {
        const node = edge.node;
        if (node.status !== 'READY') {
            logger.error('there is media not in READY status yet for ' + product_id + ' status: ' + node.status);
            logger.trace(JSON.stringify(node, null, 2));
            return false;
        }
        let key = null;
        if (node.mediaContentType === 'IMAGE') {
            if (first_video_key) {
                logger.error('unexpected media order, maybe reorder ran before for ' + product_id);
                return false;
            }
            const parts = node.alt.split('/');
            if (type === 'settings') {
                parts.pop();
                key = parts.join('/');
            } else if (type === 'wedding-rings' || type === 'jewelry') {
                key = parts[0];
            }
            //logger.trace('image key for ' + product_id + ': ' + key);
            if (!images[key]) {
                images[key] = {id: node.id};
                key_images_order.push(key);
            } else {
                images[node.alt] = {id: node.id};
                key_images_order.push(node.alt);
            }
        } else if (node.mediaContentType === 'VIDEO') {
            key = node.alt;
            videos[key] = {id: node.id};
            if (!first_video_key) {
                first_video_key = key;
            }
        }
    }
    if (!first_video_key) {
        logger.info('there is no video to reorder for ' + product_id);
        return true;
    }
    let position = 0;
    const moves = [];
    for (let i = 0; i < key_images_order.length; i++) {
        const key = key_images_order[i];
        position++;
        const video = videos[key];
        if (!video) {
            continue;
        }
        video.newPosition = position;
        moves.push({id: video.id, newPosition: position});
        position++;
    }
    if (videos[first_video_key].newPosition !== 1) {
        logger.log('move to the first video to the beginning ' + product_id);
        const image = images[first_video_key];
        const video = videos[first_video_key];
        moves.push({id: image.id, newPosition: 0});
        moves.push({id: video.id, newPosition: 1});
    }
    //logger.trace('moves: ' + JSON.stringify(moves));
    const move_result = await reorder_media_graphql(client, graphql_api_id, moves);
    if (move_result && move_result.data && move_result.data.productReorderMedia && move_result.data.productReorderMedia.mediaUserErrors &&
        Array.isArray(move_result.data.productReorderMedia.mediaUserErrors) && move_result.data.productReorderMedia.mediaUserErrors.length === 0) {
        return true;
    } else {
        logger.info('move_result', JSON.stringify(move_result, null, 2));
        logger.error('unexpected move_result, expecting move_result.data.productReorderMedia.mediaUserErrors is empty for ' + product_id);
        return false;
    }
}

async function reorder_media_graphql(client, graphql_api_id, moves) {
    if (!moves || !Array.isArray(moves) || moves.length === 0 || !moves[0].id) {
        logger.error('moves must be an array of {id, newPosition} objects');
        return false;
    }
    for (const move of moves) {
        if (typeof move.newPosition !== 'string') {
            move.newPosition = String(move.newPosition);
        }
    }
    const query_template = `mutation reorderProductMedia($graphql_api_id: ID!, $moves: [MoveInput!]!) {
      productReorderMedia(id: $graphql_api_id, moves: $moves) {
        job {
          id
          done
        }
        mediaUserErrors {
          code
          field
          message
        }
      }
    }`;
    const query = query_template.replace(/(\n|\r)/gm, ' ').replace(/ +(?= )/g,'');
    const variables = {graphql_api_id, moves};
    const options = get_axios_options(client, 'post', `/admin/api/${shopify_api_version}/graphql.json`, null, {variables, query});
    const response = await axios_multi_tries(options);
    if (response) {
        return response.data;
    } else {
        return null; 
    }
}

async function get_all_media_graphql(client, graphql_api_id, total = 100) {
    const query_template = `query get_media($graphql_api_id: ID!, $total: Int) {
      product(id: $graphql_api_id) {
        handle
        title
        media(first:$total ) {
          edges {
            node {
              ... fieldsForMediaTypes
            }
          }
        }
      }
    }
    fragment fieldsForMediaTypes on Media {
      alt
      mediaContentType
      status
      ... on Video {
        id
        sources {
          format
          height
          mimeType
          url
          width
        }
        originalSource {
          format
          height
          mimeType
          url
          width
        }
      }
      ... on MediaImage {
        id
        image {
          altText
          src
          originalSrc
        }
      }
    }`;
    const query = query_template.replace(/(\n|\r)/gm, ' ').replace(/ +(?= )/g,'');
    const variables = {graphql_api_id, total};
    const options = get_axios_options(client, 'post', `/admin/api/${shopify_api_version}/graphql.json`, null, {variables, query});
    const response = await axios_multi_tries(options);
    if (response) {
        return response.data;
    } else {
        return null; 
    }
}

async function upload_video(client, product_id, source_url, alt) {
    if (!source_url.endsWith('mp4')) {
        logger.error('unsupported extension, only mp4 is allowed');
        return false;
    }
    const parts = source_url.split('/');
    const filename = parts.pop();
    const foldername =  parts.pop();
    const sequence_num = String(new Date().getTime());
    const filepath = os.tmpdir() + '/' + sequence_num + '-' + foldername + '-' + filename;
    let media_id = null;
    for (let i = 0; i < 3; i++) {
        if (!await http_download(source_url, filepath)) {
            logger.error('failed to download the file from ' + source_url);
            return false;
        }
        if (!fs.existsSync(filepath)) {
            logger.error('downloaded file not found: ' + filepath);
            return false;
        }
        const graphql_api_id = 'gid://shopify/Product/' + product_id;
        const url = await upload_video_to_stage(client, filepath);
        if (url) {
            const result = await add_videos_graphql(client, graphql_api_id, [{ url, alt }]);
            if (result && result.data && result.data.productCreateMedia && result.data.productCreateMedia.media && 
                Array.isArray(result.data.productCreateMedia.media) && result.data.productCreateMedia.media.length === 1 && 
                result.data.productCreateMedia.media[0].id) {
                media_id = result.data.productCreateMedia.media[0].id;
            } else {
                logger.error('unexpected result, expecting result.data.productCreateMedia.media[0].id: ' + product_id);
                logger.info(JSON.stringify(result, null, 2));
            }
        } else {
            logger.error('failed to upload_video_to_stage: ' + product_id);
        }
        if (fs.existsSync(filepath)) {
            fs.unlinkSync(filepath);
        }
        if (media_id) {
            break;
        }
        await sleep(30000);
    }
    return media_id;
}

async function upload_video_to_stage(client, filepath) {
    const parts =  filepath.split('/');
    const filename = parts.pop();
    const path = parts.join('/');
    const result = await get_staged_urls_graphql(client, [filename], path);
    //logger.trace(JSON.stringify(result, null, 2));
    if (!result || !result.data || !result.data.stagedUploadsCreate || !result.data.stagedUploadsCreate.stagedTargets ||
        !Array.isArray(result.data.stagedUploadsCreate.stagedTargets || result.data.stagedUploadsCreate.stagedTargets.length === 0)) {
        logger.error('unexpected result, expecting result.data.stagedUploadsCreate.stagedTargets is an array with 1 element');
        logger.info(JSON.stringify(result, null, 2));
        return false;
    }
    const target = result.data.stagedUploadsCreate.stagedTargets[0];
    logger.trace('resourceUrl: ' + target.resourceUrl);
    const param_hash = {};
    for (const param of target.parameters) {
        param_hash[param.name] = param.value;
    }
    const args = [];
    args.push('-v');
    args.push(target.url);
    args.push('-F');
    args.push('bucket=' + param_hash.bucket);
    args.push('-F');
    args.push('key=' + param_hash.key);
    args.push('-F');
    args.push('policy=' + param_hash.policy);
    args.push('-F');
    args.push('cache-control=public, max-age=31536000');
    args.push('-F');
    args.push('x-amz-algorithm=' + param_hash['x-amz-algorithm']);
    args.push('-F');
    args.push('x-amz-signature=' + param_hash['x-amz-signature']);
    args.push('-F');
    args.push('x-amz-credential=' + param_hash['x-amz-credential']);
    args.push('-F');
    args.push('x-amz-date=' + param_hash['x-amz-date']);
    args.push('-F');
    args.push('file=@/' + filepath);
    const data = await spawn_cmd('curl', args);
    if (data.status === 0) {
      return target.resourceUrl;
    }
    return null;
}

async function get_staged_urls_graphql(client, filenames, path = '.') {
    if (!filenames || !Array.isArray(filenames) || filenames.length === 0) {
      logger.error('input must be an array of filenames');
      return false;
    }
    const input = [];
    for (const filename of filenames) {
        if (!filename) {
            logger.error('empty filename');
            return false;
        }
        if (!filename.endsWith('.mp4')) {
            logger.error('unsupported extension, only mp4 allowed: ' + filename);
            return false;
        }
        if (!fs.existsSync(path + '/' + filename)) {
            logger.error('file not found: ' + '/' + filename);
            return false;
        }
        const item = {filename, mimeType: 'video/mp4', resource: 'VIDEO'};
        const stats = fs.statSync(path + '/' + filename);
        item.fileSize = String(stats.size);
        input.push(item);
    }
    const query_template = `mutation generateStagedUploads($input: [StagedUploadInput!]!) {
      stagedUploadsCreate(input: $input) {
        stagedTargets {
          url
          resourceUrl
          parameters {
            name
            value
          }
        }
      }
    }`;
    const query = query_template.replace(/(\n|\r)/gm, ' ').replace(/ +(?= )/g,'');
    const variables = {input};
    const options = get_axios_options(client, 'post', `/admin/api/${shopify_api_version}/graphql.json`, null, {variables, query});
    const response = await axios_multi_tries(options);
    if (response) {
        return response.data;
    } else {
        return null; 
    }
}

async function add_videos_graphql(client, graphql_api_id, videos) {
    if (!videos || !Array.isArray(videos) || videos.length === 0 || !videos[0].url) {
      logger.error('videos must be an array of {url, alt} objects');
      return false;
    }
    const media = [];
    for (const video of videos) {
      const item = {
        originalSource: video.url,
        mediaContentType: 'VIDEO'
      };
      if (video.alt) {
        item.alt = video.alt;
      }
      media.push(item);
    }
    const query_template = `mutation add_media($graphql_api_id: ID!, $media: [CreateMediaInput!]!) {
      productCreateMedia(productId: $graphql_api_id, media: $media) {
        media {
          ... fieldsForMediaTypes
          mediaErrors {
            code
            details
            message
          }
        }
        product {
          id
        }
        mediaUserErrors {
          code
          field
          message
        }
      }
    }
    
    fragment fieldsForMediaTypes on Media {
      alt
      mediaContentType
      status
      ... on Video {
        id
        sources {
          format
          height
          mimeType
          url
          width
        }
        originalSource {
          format
          height
          mimeType
          url
          width
        }
      }
    }`;
    const query = query_template.replace(/(\n|\r)/gm, ' ').replace(/ +(?= )/g,'');
    const variables = {graphql_api_id, media};
    const options = get_axios_options(client, 'post', `/admin/api/${shopify_api_version}/graphql.json`, null, {variables, query});
    const response = await axios_multi_tries(options);
    if (response) {
        return response.data;
    } else {
        return null; 
    }
}

function spawn_cmd(cmd, args = null, timeout = 600, console_print = false) {
  return new Promise(resolve => {
      const data = {
          status: -1,
          stdout: '',
          stderr: '',
      };
      const result = spawn(cmd, args);
      const timer = setTimeout(() => {
          if (console_print) console.error('process timeout');
          data.stderr = 'process timeout';
          result.kill();
          resolve(data);
      }, timeout * 1000);  
      result.stdout.on('data', result => {
          const msg = result.toString();
          if (console_print) stdout.write(msg);
          data.stdout += msg;
      });
      result.stderr.on('data', result => {
          const msg = result.toString();
          if (console_print) stderr.write(msg);
          data.stderr += msg;
      });
      result.on('exit', code => {
          data.status = code;
          clearTimeout(timer);
          resolve(data);
      });
  });
}

async function get_product(client, product_id, query) {
    try {
        const options = get_axios_options(client, 'get', `/admin/api/${shopify_api_version}/products/${product_id}.json`, query);
        const response = await axios_multi_tries(options);
        return response.data;
    } catch(err) {
        logger.error(err.message);
        return false;
    }
}

async function get_collection(client, collection_id, query) {
    try {
        const options = get_axios_options(client, 'get', `/admin/api/${shopify_api_version}/collections/${collection_id}.json`, query);
        const response = await axios_multi_tries(options);
        return response.data;
    } catch(err) {
        logger.error(err.message);
        return false;
    }
}

async function get_images(client, product_id, query) {
    try {
        const options = get_axios_options(client, 'get', `/admin/api/${shopify_api_version}/products/${product_id}/images.json`, query);
        const response = await axios_multi_tries(options);
        return response.data;
    } catch(err) {
        logger.error(err.message);
        return false;
    }
}

async function find_product(client, query) {
    try {
        const options = get_axios_options(client, 'get', `/admin/api/${shopify_api_version}/products.json`, query);
        const response = await axios_multi_tries(options);
        return response.data;
    } catch(err) {
        logger.error(err.message);
        return false;
    }
}

async function get_product_metafields(client, product_id, query) {
    const q = prepare_query(query);
    const url = `/admin/api/${shopify_api_version}/products/${product_id}/metafields.json`;
    return get_all_items(client, url, q);
}

async function post_product(client, product) {
    const options = get_axios_options(client, 'post', `/admin/api/${shopify_api_version}/products.json`, null, product);
    const response = await axios_multi_tries(options);
    if (response) {
        return response.data;
    } else {
        return null; 
    }
}

async function get_all_webhooks(client, query) {
    const q = prepare_query(query);
    const url = `/admin/api/${shopify_api_version}/webhooks.json`;
    return get_all_items(client, url, q);
}

async function delete_webhook(client, id) {
    const options = get_axios_options(client, 'delete', `/admin/api/${shopify_api_version}/webhooks/${id}.json`);
    const response = await axios_multi_tries(options);
    if (response) {
        return response.data;
    } else {
        return false; 
    }
}

async function subscribe_webhook_event(client, topic, url = null) {
    const data = { webhook: { topic, address: url ? url : aws_events_arn, format: "json" }};
    const options = get_axios_options(client, 'post', `/admin/api/${shopify_api_version}/webhooks.json`, null, data);
    const response = await axios_multi_tries(options);
    if (response) {
        return response.data;
    } else {
        return false; 
    }
}

async function put_product(client, product_id, data) {
    const options = get_axios_options(client, 'put', `/admin/api/${shopify_api_version}/products/${product_id}.json`, null, data);
    const response = await axios_multi_tries(options);
    if (response) {
        return response.data;
    } else {
        return null; 
    }
}

async function post_product_metafields(client, product_id, data) {
    const url = `/admin/api/${shopify_api_version}/products/$product_id}/metafields.json`;
    const options = get_axios_options(client, 'post', url, null, data);
    const response = await axios_multi_tries(options);
    return (response != null);
}

async function get_variant_metafields(client, product_id, variant_id, query) {
    const q = prepare_query(query);
    const url = `/admin/api/${shopify_api_version}/products/${product_id}/variants/${variant_id}/metafields.json`;
    return get_all_items(client, url, q);
}

async function post_variant_metafields(client, product_id, variant_id, data) {
    const url = `/admin/api/${shopify_api_version}/products/${product_id}/variants/${variant_id}/metafields.json`;
    const options = get_axios_options(client, 'post', url, null, data);
    const response = await axios_multi_tries(options);
    return (response != null);
    
}

async function get_all_products(client, query) {
    const q = prepare_query(query);
    const url = `/admin/api/${shopify_api_version}/products.json`;
    return get_all_items(client, url, q);
}

async function get_metafields(client, query) {
    const q = prepare_query(query);
    const url = `/admin/api/${shopify_api_version}/metafields.json`;
    return get_all_items(client, url, q);
}

async function get_all_smart_collections(client, query) {
    const q = prepare_query(query);
    const url = `/admin/api/${shopify_api_version}/smart_collections.json`;
    return get_all_items(client, url, q);
}

async function get_all_custom_collections(client, query) {
    const q = prepare_query(query);
    const url = `/admin/api/${shopify_api_version}/custom_collections.json`;
    return get_all_items(client, url, q);
}

async function get_all_collects(client, query) {
    const q = prepare_query(query);
    const url = `/admin/api/${shopify_api_version}/collects.json`;
    return get_all_items(client, url, q);
}

async function delete_all_metafields(client, query) {
    const products = await get_all_products(client, query);
    for (const product of products) {
        const metafields = await get_product_metafields(product.id);
        for (const metafield of metafields) {
            const url =`/admin/api/${shopify_api_version}/metafields/${metafield.id}.json`;
            const options = get_axios_options(client, 'delete', url);
            await axios_multi_tries(options);
        }
        for (const variant of product.variants) {
            const variant_metafields = await get_variant_metafields(client, product.id, variant.id);
            for (const metafield of variant_metafields) {
                const url =`/admin/api/${shopify_api_version}/metafields/${metafield.id}.json`;
                const options = get_axios_options(client, 'delete', url);
                await axios_multi_tries(options);
            }
        }
    }
}

async function get_products_count(client) {
    const options = get_axios_options(client, 'get', `/admin/api/${shopify_api_version}/products/count.json`);
    const response = await axios_multi_tries(options);
    if (response) {
        return response.data;
    } else {
        return false; 
    }
}

async function get_smart_collections_count(client) {
    const options = get_axios_options(client, 'get', `/admin/api/${shopify_api_version}/smart_collections/count.json`);
    const response = await axios_multi_tries(options);
    if (response) {
        return response.data;
    } else {
        return false; 
    }
}

async function get_custom_collections_count(client) {
    const options = get_axios_options(client, 'get', `/admin/api/${shopify_api_version}/custom_collections/count.json`);
    const response = await axios_multi_tries(options);
    if (response) {
        return response.data;
    } else {
        return false; 
    }
}

async function get_next_page_products(client, cursor, query) {
    const q = prepare_query(query);
    const url = `/admin/api/${shopify_api_version}/products.json`;
    return get_next_page_items(client, url, q, cursor);
}

async function get_next_page_collects(client, cursor, query) {
    const q = prepare_query(query);
    const url = `/admin/api/${shopify_api_version}/collects.json`;
    return get_next_page_items(client, url, q, cursor);
}

async function get_collection_metafields(client, collection_id, query) {
    const q = prepare_query(query);
    const url = `/admin/api/${shopify_api_version}/collections/${collection_id}/metafields.json`;
    return get_all_items(client, url, q);
}

async function get_next_page_smart_collections(client, cursor, query) {
    const q = prepare_query(query);
    const url = `/admin/api/${shopify_api_version}/smart_collections.json`;
    return get_next_page_items(client, url, q, cursor);
}

async function get_next_page_custom_collections(client, cursor, query) {
    const q = prepare_query(query);
    const url = `/admin/api/${shopify_api_version}/custom_collections.json`;
    return get_next_page_items(client, url, q, cursor);
}

async function delete_all_products(client, query, max_promises = 4) {
    if (query) {
        query.fields = 'id,title';
    } else {
        query = {fields: 'id,title'};
    }
    let last_tries = 0;
    let cursor = {};
    const promises = [];
    while (last_tries <= 3) {
        const products = await get_next_page_products(client, cursor, query);
        if (!products || products.length === 0) {
            cursor = {};
            last_tries++;
            await sleep(10000);
            continue;
        }
        last_tries = 0;
        for (const product of products) {
            logger.info('deleting ' + product.id + ' ' + product.title);
            promises.push(delete_product(client, product.id));
            if (promises.length >= max_promises) {
                await Promise.all(promises);
                promises.length = 0;
            }
        }
    }
    if (promises.length > 0) {
        await Promise.all(promises);
    }
}

async function bulk_update_inventory_items(client, query, update, max_promises = 4) {
    if (query) {
        query.fields = 'id,title,variants';
    } else {
        query = {fields: 'id,title,variants'};
    }
    const cursor = {};
    const promises = [];
    // eslint-disable-next-line no-constant-condition
    while (true) {
        const products = await get_next_page_products(client, cursor, query);
        if (!products || products.length === 0) {
            break;
        }
        for (const product of products) {
            for (const variant of product.variants) {
                promises.push(update_inventory_item(client, variant.inventory_item_id, update));
                if (promises.length >= max_promises) {
                    await Promise.all(promises);
                    promises.length = 0;
                }
            }
        }
    }
    if (promises.length > 0) {
        await Promise.all(promises);
    }
}

async function bulk_update_products(client, query, update, max_promises = 4) {
    if (query) {
        query.fields = 'id,title,variants';
    } else {
        query = {fields: 'id,title,variants'};
    }
    const cursor = {};
    const promises = [];
    // eslint-disable-next-line no-constant-condition
    while (true) {
        const products = await get_next_page_products(client, cursor, query);
        if (!products || products.length === 0) {
            break;
        }
        for (const product of products) {
            //logger.trace(JSON.stringify(product, null, 2));
            logger.info('update ' + product.id + ' ' + product.title);
            const product_update = {};
            const variants_update = {};
            for (const key in update) {
                if (key.startsWith('variant_')) {
                    variants_update[key.substr(8)] = update[key];
                } else {
                    product_update[key] = update[key];
                }
            }
            if (Object.keys(variants_update).length > 0) {
                for (const variant of product.variants) {
                    for (const key in variants_update) {
                        variant[key] = variants_update[key];
                    }
                }
                product_update.variants = product.variants;
            }
            //logger.trace(JSON.stringify(product_update, null, 2));
            promises.push(put_product(client, product.id, {product: update}));
            if (promises.length >= max_promises) {
                await Promise.all(promises);
                promises.length = 0;
            }
        }
    }
    if (promises.length > 0) {
        await Promise.all(promises);
    }
}

async function delete_product(client, product_id) {
    const options = get_axios_options(client, 'delete', `/admin/api/${shopify_api_version}/products/${product_id}.json`);
    const response = await axios_multi_tries(options);
    if (response) {
        return response.data;
    } else {
        return false; 
    }
}

async function get_all_items(client, url, query) {
    const items = [];
    while (url) {
        //let ok = true;
        const options = get_axios_options(client, 'get', url, query);
        const response = await axios_multi_tries(options);
        if (!response || response.status !== 200) {
            logger.error('job completed with error(s), some items may missing.');
            break;
        }
        let count = 0;
        for (const name in response.data) {
            count++;
            for (const item of response.data[name]) {
                items.push(item);
            }
        }
        if (count > 1) {
            logger.error('unexpected count(' + count + ') of keys in response data: ' +  url);
        }
        url = get_next_url(response.headers.link);
        query = null;
    }
    return items;
}

async function update_inventory_item(client, inventory_item_id, update) {
    update.id = inventory_item_id;
    const data = {
        inventory_item: update
    };
    const options = get_axios_options(client, 'put', `/admin/api/${shopify_api_version}/inventory_items/${inventory_item_id}.json`, null, data);
    const response = await axios_multi_tries(options);
    if (response) {
        return response.data;
    } else {
        return false; 
    }
}

async function update_product_tags(client, product_id, tags) {
    const product = { tags };
    const result = await put_product(client, product_id, {product});
    //logger.trace(result);
    return result;
}

async function update_product_body_html(client, product_id, body_html) {
    const product = { body_html };
    const result = await put_product(client, product_id, {product});
    //logger.trace(result);
    return result;
}

async function update_variant_metafields(client, product_id, variant_id, metafields, to_delete = null, dryrun = false) {
    if (to_delete) {
        if (!dryrun) {
            for (const metafield of to_delete) {
                await delete_metafield(client, metafield.id);
            }
        } else {
            logger.info(product_id, variant_id, 'delete', JSON.stringify(to_delete, null, 2));
        }
    }
    if (!dryrun) {
        for (const metafield of metafields) {
            await post_variant_metafields(client, product_id, variant_id, metafield);
        }
    } else {
        logger.info(product_id, variant_id, 'post', JSON.stringify(metafields, null, 2));
    }
}

async function update_product_metafields(client, product_id, updates, namespace = 'detail', delete_extra = false, dryrun = false) {
    const metafields = await get_product_metafields(client, product_id);
    if (dryrun) {
        logger.info('current product metafields:', JSON.stringify(metafields, null, 2));
    }
    if (!metafields || metafields.length === 0) {
        const metafields = [];
        for (const key in updates) {
            if (key.length > 30) {
                logger.error(key + ' is too long for key(10)');
            }
            let value_type = 'string';
            let value = String(updates[key]);
            if (typeof updates[key] === 'object') {
                value_type = 'json_string';
                value = JSON.stringify(updates[key]);
            }
            metafields.push({
                namespace, key, value, value_type
            });
        }
        const data = {product: { metafields }};
        if (!dryrun) {
            await put_product(client, product_id, data);
        } else {
            logger.info(product_id, namespace, JSON.stringify(metafields, null, 2));
        }
        return;
    }
    const clone = JSON.parse(JSON.stringify(updates));
    for (const key in updates) {
        if (key.length > 30) {
            logger.error(key + ' is too long for key(11)');
        }
        for (const metafield of metafields) {
            if (metafield.key === key) {
                let value_type = 'string';
                let value = String(updates[key]);
                if (typeof updates[key] === 'object') {
                    value_type = 'json_string';
                    value = JSON.stringify(updates[key]);
                }
                if (value !== metafield.value) {
                    const data = {
                        metafield: { id: metafield.id, value, value_type }
                    };
                    if (!dryrun) {
                        const url = `/admin/api/${shopify_api_version}/products/${product_id}/metafields/${metafield.id}.json`;
                        const options = get_axios_options(client, 'put', url, null, data);
                        await axios_multi_tries(options);
                    } else {
                        logger.info(product_id, namespace, JSON.stringify(data.metafield, null, 2));
                    }
                }
                delete clone[key];
                break;
            }
        }
    }
    for (const key in clone) {
        if (key.length > 30) {
            logger.error(key + ' is too long for key(12)');
        }
        let value_type = 'string';
        let value = String(clone[key]);
        if (typeof clone[key] === 'object') {
            value_type = 'json_string';
            value = JSON.stringify(clone[key]);
        }
        const data = {
            metafield: { namespace, key, value, value_type }
        };
        if (!dryrun) {
            const url = `/admin/api/${shopify_api_version}/products/${product_id}/metafields.json`;
            const options = get_axios_options(client, 'post', url, null, data);
            // eslint-disable-next-line no-unused-vars
            const response = await axios_multi_tries(options);
            //logger.trace(response.data);
        } else {
            logger.info(product_id, namespace, JSON.stringify(data.metafield, null, 2));
        }
    }
    if (delete_extra) {
        const extra = {};
        for (const metafield of metafields) {
            if (metafield.namespace !== namespace) {
                continue;
            }
            if (!updates[metafield.key]) {
                extra[metafield.key] = metafield.id;
            }
        }
        for (const key in extra) {
            const metafield_id = extra[key];
            if (!dryrun) {
                const url = `/admin/api/${shopify_api_version}/products/${product_id}/metafields/${metafield_id}.json`;
                const options = get_axios_options(client, 'delete', url);
                await axios_multi_tries(options);
            } else {
                logger.info(product_id, namespace, 'delete', key, metafield_id);
            }
        }
    }
}

async function delete_metafield(client, metafield_id) {
    const url = `/admin/api/${shopify_api_version}/metafields/${metafield_id}.json`;
    const options = get_axios_options(client, 'delete', url);
    await axios_multi_tries(options);
}

async function get_next_page_items(client, url, query, cursor) {
    if (cursor && cursor.hasOwnProperty('url')) {
        url = cursor.url;
        query = null;
    }
    if (!url) {
        return false;
    }
    const options = get_axios_options(client, 'get', url, query);
    const response = await axios_multi_tries(options);
    if (!response || response.status !== 200) {
        logger.error('job completed with error(s), some items may missing.');
        return null;
    }
    if (cursor) {
        cursor.url = get_next_url(response.headers.link);
    }
    const key = Object.keys(response.data)[0];
    return response.data[key];
}

async function axios_multi_tries(options) {
    //logger.trace(options);
    const is_graphql = options.url.indexOf('/graphql.json') > 0;
    for (let i = 0; i < 9; i++) {
        try {
            const response = await axios(options);
            if (is_graphql && response && response.data && response.data.errors && 
                Array.isArray(response.data.errors) && response.data.errors.length > 0 &&
                response.data.errors[0].message === 'Throttled') {
                logger.warn('graphql call throttled');
                await sleep(1000*(i*i+1));
                continue;
            }
            return response;
        } catch(err) {
            logger.warn(err.message);
            if (err.response) {
                const status = err.response.status;
                if (status === 429 || status >= 500) {
                    await sleep(1000*(i*i+1));
                } else {
                    logger.error(err.response.data);
                    break;
                }
            } else {
                break;
            }
        }
    }
    logger.error('failed request for ' + options.url);
    return null;
}

function get_random_int(min, max) {
    return Math.floor(min + Math.random() * Math.floor(max - min));
}

let multiple_apps_indexes = {};

function get_axios_options(client, method, api_url, query, data) {
    if (!client) {
        logger.error('client is empty');
        return null;
    }
    let shopify_url;
    if (api_url.startsWith('https')) {
        shopify_url = api_url;
    } else if (client.store_url) {
        shopify_url = client.store_url + api_url;
    } else {
        logger.error('missing store_url in store_info');
        return null;
    }
    const has_page_info = shopify_url.indexOf('page_info=') !== -1;
    if (query) {
        let search = '';
        for (const key in query) {
            if (search.length === 0) {
                search += '?';
            } else {
                search += '&';
            }
            search += encodeURIComponent(key) + '=' + encodeURIComponent(query[key]);
        }
        shopify_url += search;
    }
    const headers = { accept: 'application/json' };
    if (!has_page_info && client.multiple_apps) {
        const store_name = client.store_name;
        const length = client.multiple_apps.length;
        let done  = false;
        if (!multiple_apps_indexes.hasOwnProperty(store_name)) {
            multiple_apps_indexes[store_name] = get_random_int(0, length - 1);
        } else if (multiple_apps_indexes[store_name] === length) {
            multiple_apps_indexes[store_name] = 0;
            if (client.store_access_token) {
                done = true;
                headers['X-Shopify-Access-Token'] = client.store_access_token;
            }
        }
        if (!done) {
            const app = client.multiple_apps[multiple_apps_indexes[store_name]++];
            const url = new URL(shopify_url);
            url.username = app.store_api_key;
            url.password = app.store_password;
            shopify_url = url.toString();
        }
    } else if (client.store_access_token) {
        headers['X-Shopify-Access-Token'] = client.store_access_token;
    } else if (client.store_api_key && client.store_password) {
        const url = new URL(shopify_url);
        url.username = client.store_api_key;
        url.password = client.store_password;
        shopify_url = url.toString();
    } else {
        logger.error('missing access credential in store_info');
        return null;
    }
    const options = {method: 'get', url: shopify_url, headers, timeout: 180000};
    if (method) {
        options.method = method;
    } else if (data) {
        options.method = 'post';
    }
    if (data) {
        options.data = data;
    }
    if (http_request_timeout) {
        options.timeout = http_request_timeout;
    }
    return options;
}

function get_next_url(link) {
    if (!link) {
        return null;
    }
    let parts = link.split(', ');
    if (parts.length === 1) {
        parts = parts[0].split('; ');
    } else if (parts.length === 2) {
        if (parts[0].includes('rel="next"')) {
            parts = parts[0].split('; ');
        } else {
            parts = parts[1].split('; ');
        }
    } else {
        return null;
    }
    if (parts.length === 2 && parts[0].startsWith('<') && parts[0].endsWith('>') && parts[1].includes('rel="next"')) {
        return parts[0].substr(1, parts[0].length-2);
    }
    return null;
}

// eslint-disable-next-line no-unused-vars
function get_prev_url(link) {
    if (!link) {
        return null;
    }
    let parts = link.split(', ');
    if (parts.length === 1) {
        parts = parts[0].split('; ');
    } else if (parts.length === 2) {
        if (parts[0].includes('rel="previous"')) {
            parts = parts[0].split('; ');
        } else {
            parts = parts[1].split('; ');
        }
    } else {
        return null;
    }
    if (parts.length === 2 && parts[0].startsWith('<') && parts[0].endsWith('>') && parts[1].includes('rel="previous"')) {
        return parts[0].substr(1, parts[0].length-2);
    }
    return null;
}

function prepare_query(query) {
    if (query) {
        const q = JSON.parse(JSON.stringify(query));
        if (!q.limit) {
            q.limit = 250;
        }
        return q;
    } else {
        return { limit: 250 };
    }
}

function sleep(milliseconds) {
    return new Promise(resolve => setTimeout(resolve, milliseconds));
}

function http_download(url, local_file_pathname) {
    return new Promise((resolve) => {
        axios({
            url: url,
            method: 'GET',
            headers: { Accept: '*/*' },
            responseType: 'stream',
            httpsAgent: new https.Agent({rejectUnauthorized: false}),
            timeout: 6000
        }).then(response => {
            const writer = createWriteStream(local_file_pathname);
            response.data.pipe(writer);
            writer.on('finish', () => { 
                resolve(true); 
            });
            writer.on('error', () => {
                resolve('failed to stream to file for http_download: ' + url);
            });
        }).catch(err => {
            console.error(err.message + ' for ' + url);
            if (err.response && err.response.data) {
                console.error(err.response.headers);
                //console.error(err.response.data);
            } else if (err.request) {
                console.error(err.request);
            }
            resolve(err.message);
        });
    });
}