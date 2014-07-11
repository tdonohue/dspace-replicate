/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree and available online at
 *
 * http://www.dspace.org/license/
 */
package org.dspace.ctask.replicate.store;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import org.apache.log4j.Logger;

import org.dspace.core.ConfigurationManager;
import org.dspace.ctask.replicate.ObjectStore;
import org.dspace.curate.Utils;

import org.duracloud.client.ContentStore;
import org.duracloud.client.ContentStoreManager;
import org.duracloud.client.ContentStoreManagerImpl;
import org.duracloud.common.model.Credential;
import org.duracloud.common.retry.ExceptionHandler;
import org.duracloud.common.retry.Retriable;
import org.duracloud.common.retry.Retrier;
import org.duracloud.domain.Content;
import org.duracloud.error.ContentStoreException;
import org.duracloud.error.NotFoundException;

/**
 * DuraCloudObjectStore interacts with DuraCloud using its RESTful web service API
 * (via its java client library)
 *
 * @author richardrodgers
 * @author tdonohue
 */
public class DuraCloudObjectStore implements ObjectStore
{
    private Logger log = Logger.getLogger(DuraCloudObjectStore.class);
    
    // DuraCloud store
    private ContentStore dcStore = null;
    
    public DuraCloudObjectStore()
    {
    }

    /**
     * Initialize object storage by ensuring the connection to DuraCloud works.
     * This actually authenticates to DuraCloud using the info in your
     * duracloud.cfg configuration file.
     * 
     * @throws IOException 
     */
    @Override
    public void init() throws IOException
    {
        // locate & login to Duracloud store
        ContentStoreManager storeManager =
            new ContentStoreManagerImpl(localProperty("host"),
                                        localProperty("port"),
                                        localProperty("context"));
        Credential credential = 
            new Credential(localProperty("username"), localProperty("password"));
        storeManager.login(credential);
        try
        {
            //Get the primary content store (e.g. Amazon)   
            dcStore = storeManager.getPrimaryContentStore();
        }
        catch (ContentStoreException csE)
        {      
            throw new IOException("Unable to connect to the DuraCloud Primary Content Store. Please check the DuraCloud connection/authentication settings in your 'duracloud.cfg' file.", csE);
        }
    }

    /**
     * Download a specified object (based on 'id') from DuraCloud, saving
     * its contents to a local file.
     * @param group Name of 'space' in DuraCloud where object exists
     * @param id ID/Name of Object to download
     * @param file Local file to save contents to
     * @return size of file downloaded
     * @throws IOException 
     */
    @Override
    public long fetchObject(String group, String id, File file) throws IOException
    {
        long size = 0L;
        try
        {
            // Download content from DuraCloud
            Content content = dcStore.getContent(getSpaceID(group), getContentPrefix(group) + id);
            // Get size of downloaded file
            size = Long.valueOf(content.getProperties().get(ContentStore.CONTENT_SIZE));
            
            // Write to a local file
            FileOutputStream out = new FileOutputStream(file);
            InputStream in = content.getStream();
            Utils.copy(in, out);
            in.close();
            out.close();
        }
        catch (NotFoundException nfE)
        {
            // no object - no-op
        }
        catch (ContentStoreException csE)
        {
            throw new IOException(csE);
        }
        return size;
    }
    
    /**
     * Check if an object exists in DuraCloud
     * @param group Name of 'space' in DuraCloud where object may exist
     * @param id ID/Name of Object to look for
     * @return true if exists, false otherwise
     * @throws IOException 
     */
    @Override
    public boolean objectExists(String group, String id) throws IOException
    {
        try
        {
            return dcStore.getContentProperties(getSpaceID(group), getContentPrefix(group) + id) != null;
        }
        catch (NotFoundException nfE)
        {
            return false;
        }
        catch (ContentStoreException csE)
        {
            throw new IOException(csE);
        }
    }

    /**
     * Delete a specified object from DuraCloud
     * @param group Name of 'space' in DuraCloud where object exists
     * @param id ID/Name of Object to delete
     * @return size of file deleted
     * @throws IOException 
     */
    @Override
    public long removeObject(String group, String id) throws IOException
    {
        // get metadata before blowing away
        long size = 0L;
        try
        {
            Map<String, String> attrs = dcStore.getContentProperties(getSpaceID(group), getContentPrefix(group) + id);
            size = Long.valueOf(attrs.get(ContentStore.CONTENT_SIZE));
            dcStore.deleteContent(getSpaceID(group), getContentPrefix(group) + id);
        }
        catch (NotFoundException nfE)
        {
            // no replica - no-op
        }
        catch (ContentStoreException csE)
        {
            throw new IOException(csE);
        }
        return size;
    }

    /**
     * Upload/transfer a local file to DuraCloud
     * @param group Name of 'space' in DuraCloud where object should be saved
     * @param file Local file to upload
     * @return size of file deleted
     * @throws IOException 
     */
    @Override
    public long transferObject(String group, File file) throws IOException
    {
        long size = 0L;
        String chkSum = Utils.checksum(file, "MD5");
        // make sure this is a different file from what replica store has
        // to avoid network I/O tax
        try
        {
            Map<String, String> attrs = dcStore.getContentProperties(getSpaceID(group), getContentPrefix(group) + file.getName());
            if (! chkSum.equals(attrs.get(ContentStore.CONTENT_CHECKSUM)))
            {
                size = uploadReplica(group, file, chkSum);
            }
        }
        catch (NotFoundException nfE)
        {
            // no extant replica - proceed
            size = uploadReplica(group, file, chkSum);
        }
        catch (ContentStoreException csE)
        {
            throw new IOException(csE);
        }
        // delete staging file
        file.delete();
        return size;
    }

    /**
     * Attempt (multiple times) to upload a local file to DuraCloud.
     * <P>
     * DuraCloud recommends performing multiple attempts (and even has some of 
     * its retry logic built in), just because Amazon S3 can sometimes experience
     * random request failures (i.e. network hiccups), especially with larger
     * files.
     * 
     * @param group Name of 'space' in DuraCloud where object should be saved
     * @param file Local file to upload
     * @param chkSum Checksum of local file (passed to DuraCloud for 
     *      verification of a successful upload)
     * @return size of file uploaded
     * @throws IOException 
     */
    private long uploadReplica(final String group, final File file, final String chkSum) throws IOException
    {
        try
        {
            // Initialize a DuraCloud "Retrier" to attempt the upload multiple 
            // times (3 by default), if at first it fails.
            Retrier retrier = new Retrier();
            //Run the retrier, passing it a custom Retriable & ExceptionHandler
            return retrier.execute(
                new Retriable() {
                    @Override
                    public Long retry() throws IOException
                    {
                        // This is the actual method to be retried (if it fails)
                        doUpload(group, file, chkSum);
                        
                        // If upload succeeds, return the length of the file
                        return file.length();
                    }
                }, 
                new ExceptionHandler() {
                    @Override
                    public void handle(Exception ex) {
                        // Log a custom error if a single upload fails. 
                        // This gives us a sense of how frequently failure may be happening
                        log.error("Upload of " + file.getName() + " to DuraCloud space " + group + " FAILED (but will try again a total of 3 times)", ex);
                    }
                }
            );
        }
        catch (Exception e)
        {
            throw new IOException(e);
        } 
        
    }

    /**
     * Actually perform the Upload to DuraCloud.
     * @param group Name of 'space' in DuraCloud where object should be saved
     * @param file Local file to upload
     * @param chkSum Checksum of local file (passed to DuraCloud for 
     *      verification of a successful upload)
     * @throws IOException 
     */
    private void doUpload(String group, File file, String chkSum) throws IOException
    {
        //@TODO: We shouldn't need to pass a hardcoded MIME Type. Unfortunately, 
        // DuraCloud doesn't (yet) dynamically determine a file's MIME Type.
        String mimeType = "application/octet-stream";
        if(file.getName().endsWith(".zip"))
            mimeType = "application/zip";
        else if (file.getName().endsWith(".tgz"))
            mimeType = "application/x-gzip";
        else if(file.getName().endsWith(".txt"))
            mimeType = "text/plain";

        FileInputStream fis = null;
        try
        {
            fis = new FileInputStream(file);
            dcStore.addContent(getSpaceID(group), getContentPrefix(group) + file.getName(),
                               fis, file.length(),
                               mimeType, chkSum,
                               new HashMap<String, String>());
        }
        catch (ContentStoreException csE)
        {
            throw new IOException(csE);
        }
        finally
        {
            if (fis != null)
                fis.close();
        }
     }

    /**
     * Move an object in DuraCloud from one location (space) to another
     * @param srcGroup source location (a space in DuraCloud)
     * @param destGroup destination location (another space in DuraCloud)
     * @param id ID/Name of object to move
     * @return size of moved object
     * @throws IOException 
     */
    @Override
    public long moveObject(String srcGroup, String destGroup, String id) throws IOException
    {
        // get file-size metadata before moving the content
        long size = 0L;
        try
        {
            Map<String, String> attrs = dcStore.getContentProperties(getSpaceID(srcGroup), getContentPrefix(srcGroup) + id);
            size = Long.valueOf(attrs.get(ContentStore.CONTENT_SIZE));
            dcStore.moveContent(getSpaceID(srcGroup), getContentPrefix(srcGroup) + id, 
                                getSpaceID(destGroup), getContentPrefix(destGroup) + id);
        }
        catch (NotFoundException nfE)
        {
            // no replica - no-op
        }
        catch (ContentStoreException csE)
        {
            throw new IOException(csE);
        }
        return size;
    }
    
    /**
     * Read a single attribute (i.e. property) of an object within DuraCloud
     * @param group Name of 'space' in DuraCloud where object exists
     * @param id ID/Name of object
     * @param attrName attribute (property) to return the value of
     * @return String value of specified property in DuraCloud
     * @throws IOException 
     */
    @Override
    public String objectAttribute(String group, String id, String attrName) throws IOException
    {
        try
        {
            Map<String, String> attrs = dcStore.getContentProperties(getSpaceID(group), getContentPrefix(group) + id);
            
            if ("checksum".equals(attrName))
            {
                return attrs.get(ContentStore.CONTENT_CHECKSUM);
            }
            else if ("sizebytes".equals(attrName))
            {
                return attrs.get(ContentStore.CONTENT_SIZE);
            }
            else if ("modified".equals(attrName))
            {
                return attrs.get(ContentStore.CONTENT_MODIFIED);
            }
            return null;
        }
        catch (NotFoundException nfE)
        {
            return null;
        }
        catch (ContentStoreException csE)
        {
            throw new IOException(csE);
        }
    }
    
    /**
     * Read a configuration setting from the duracloud.cfg file
     * @param name name of configuration setting to read
     * @return String value of configuration setting
     */
    private static String localProperty(String name)
    {
        return ConfigurationManager.getProperty("duracloud", name);
    }
    
    /**
     * Returns the Space ID where content should be stored in DuraCloud,
     * based on the passed in Group.
     * <P>
     * If the group contains a forward slash ('/'), then the substring
     * before the first slash is assumed to be the Space ID.
     * Otherwise, the entire group name is assumed to be the Space ID.
     * @param String group name
     * @return DuraCloud Space ID
     */
    private String getSpaceID(String group)
    {
        //If group contains a forward or backslash, then the
        //Space ID is whatever is before that slash
        if(group!=null && group.contains("/"))
            return group.substring(0, group.indexOf("/"));
        else // otherwise, the passed in group is the Space ID
            return group;
    }
    
    /**
     * Returns the Content prefix that should be used when saving a file
     * to a DuraCloud space.
     * <P>
     * If the group contains a forward slash ('/'), then the substring
     * after the first slash is assumed to be the content naming prefix.
     * Otherwise, there is no content naming prefix.
     * @param String group name
     * @return content prefix (ending with a forward slash)
     */
    private String getContentPrefix(String group)
    {
        //If group contains a forward or backslash, then the
        // content prefix is whatever is after that slash
        if(group!=null && group.contains("/"))
            return group.substring(group.indexOf("/")+1) + "/";
        else // otherwise, no content prefix is specified
            return "";
    }
}
