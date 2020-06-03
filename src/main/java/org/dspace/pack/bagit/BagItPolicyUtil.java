/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree and available online at
 *
 * http://www.dspace.org/license/
 */
package org.dspace.pack.bagit;

import java.io.IOException;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import org.dspace.authorize.AuthorizeException;
import org.dspace.authorize.ResourcePolicy;
import org.dspace.authorize.factory.AuthorizeServiceFactory;
import org.dspace.authorize.service.AuthorizeService;
import org.dspace.authorize.service.ResourcePolicyService;
import org.dspace.content.DSpaceObject;
import org.dspace.content.packager.PackageException;
import org.dspace.content.packager.PackageUtils;
import org.dspace.core.Constants;
import org.dspace.core.Context;
import org.dspace.curate.Curator;
import org.dspace.eperson.EPerson;
import org.dspace.eperson.Group;
import org.dspace.eperson.factory.EPersonServiceFactory;
import org.dspace.eperson.service.EPersonService;
import org.dspace.eperson.service.GroupService;
import org.dspace.pack.bagit.xml.policy.Policies;
import org.dspace.pack.bagit.xml.policy.Policy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Operations for {@link ResourcePolicy} objects in BagIt bags
 *
 * Generates a {@link Policy} pojo for {@link DSpaceObject}s which can be easily serialized to XML
 *
 * @author mikejritter
 */
public class BagItPolicyUtil {

    private static final Logger logger = LoggerFactory.getLogger(BagItPolicyUtil.class);

    /**
     * Create a {@link Policy} for a {@link DSpaceObject}
     *
     * @param dso The {@link DSpaceObject} to get the {@link Policy} for
     * @return the {@link Policy}
     */
    public static Policies getPolicy(final Context context, final DSpaceObject dso) throws IOException {
        final Policies policies = new Policies();
        final BiMap<Integer, String> actions = actionMapper().inverse();
        final DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

        for (ResourcePolicy resourcePolicy : dso.getResourcePolicies()) {
            final Policy policy = new Policy();

            // name and description
            policy.setName(resourcePolicy.getRpName());
            policy.setDescription(resourcePolicy.getRpDescription());

            final Date endDate = resourcePolicy.getEndDate();
            final Date startDate = resourcePolicy.getStartDate();
            if (startDate != null) {
                policy.setStartDate(dateFormat.format(startDate));
            }
            if (endDate != null) {
                policy.setEndDate(dateFormat.format(endDate));
            }

            // attributes for determining if adding policies on a group + what type of group or policies for a user
            final Group group = resourcePolicy.getGroup();
            final EPerson ePerson = resourcePolicy.getEPerson();
            if (group != null) {
                final String groupName = group.getName();
                if (groupName.equals(Group.ANONYMOUS)) {
                    policy.setGroup(Group.ANONYMOUS);
                } else if (groupName.equals(Group.ADMIN)) {
                    policy.setGroup(Group.ADMIN);
                } else {
                    try {
                        policy.setGroup(PackageUtils.translateGroupNameForExport(context, groupName));
                    } catch (PackageException exception) {
                        // since this is called by a Packer, wrap the PackageException in an IOException so it can
                        // continue to be thrown up the stack
                        throw new IOException(exception.getMessage(), exception);
                    }
                }
            } else if (ePerson != null) {
                policy.setEperson(ePerson.getEmail());
            } else {
                logger.warn("No EPerson or Group found for policy!");
            }

            final String action = actions.get(resourcePolicy.getAction());
            policy.setAction(action);

            final String type = resourcePolicy.getRpType();
            policy.setType(type);

            policies.addPolicy(policy);
        }

        return policies;
    }

    /**
     * Register all policies found from {@link Policies#getPolicies()} by mapping them to a new {@link ResourcePolicy}.
     * This operation will replace all existing ResourcePolicies for a given {@link DSpaceObject} unless there is an
     * error during the mapping from a {@link Policy} to a {@link ResourcePolicy}.
     *
     * @param dSpaceObject the {@link DSpaceObject} to register policies for
     * @param policies the {@link Policies} pojo to create each {@link ResourcePolicy}
     */
    public static void registerPolicies(final DSpaceObject dSpaceObject, final Policies policies)
        throws SQLException, AuthorizeException, PackageException {
        final DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        final GroupService groupService = EPersonServiceFactory.getInstance().getGroupService();
        final EPersonService ePersonService = EPersonServiceFactory.getInstance().getEPersonService();
        final AuthorizeService authorizeService = AuthorizeServiceFactory.getInstance().getAuthorizeService();
        final ResourcePolicyService resourcePolicyService =
            AuthorizeServiceFactory.getInstance().getResourcePolicyService();

        // Need to map policy children from List<Element> to List<ResourcePolicy>
        // then use the authorizationService to add all policies to the dso
        final List<ResourcePolicy> resourcePolicies = new ArrayList<>();
        for (Policy policy : policies.getPolicies()) {
            final ResourcePolicy resourcePolicy = resourcePolicyService.create(Curator.curationContext());

            final String rpName = policy.getName();
            if (rpName != null) {
                resourcePolicy.setRpName(rpName);
            }

            final String rpDescription = policy.getDescription();
            if (rpDescription != null) {
                resourcePolicy.setRpDescription(rpDescription);
            }

            final String rpStartDate = policy.getStartDate();
            if (rpStartDate != null) {
                try {
                    final Date date = dateFormat.parse(rpStartDate);
                    resourcePolicy.setStartDate(date);
                } catch (ParseException ignored) {
                    logger.warn("Failed to parse rp-start-date. The date needs to be in the format 'yyyy-MM-dd'.");
                }
            }

            final String rpEndDate = policy.getEndDate();
            if (rpEndDate != null) {
                try {
                    final Date date = dateFormat.parse(rpEndDate);
                    resourcePolicy.setEndDate(date);
                } catch (ParseException ignored) {
                    logger.warn("Failed to parse rp-end-date. The date needs to be in the format 'yyyy-MM-dd'.");
                }
            }

            final String groupName = policy.getGroup();
            final String epersonEmail = policy.getEperson();
            if (groupName != null) {
                final String nameForImport;

                if (groupName.equalsIgnoreCase(Group.ADMIN) || groupName.equalsIgnoreCase(Group.ANONYMOUS)) {
                    nameForImport = groupName;
                } else {
                    nameForImport = PackageUtils.translateGroupNameForImport(Curator.curationContext(), groupName);
                }

                final Group group = groupService.findByName(Curator.curationContext(), nameForImport);
                if (group == null) {
                    throw new PackageException("Could not find group " + nameForImport + " in the database! If this" +
                                               "is either the ADMIN or ANONYMOUS group check that your database is" +
                                               "initialized correctly.");
                }

                resourcePolicy.setGroup(group);
            } else if (epersonEmail != null) {
                final EPerson ePerson = ePersonService.findByEmail(Curator.curationContext(), epersonEmail);
                if (ePerson == null) {
                    throw new PackageException("Could not find ePerson " + epersonEmail + " in the database!");
                }

                resourcePolicy.setEPerson(ePerson);
            } else {
                // throw an exception as well?
                logger.warn("Cannot import policy, no rp-group or rp-eperson attribute found on value!");
            }

            final Integer action = actionMapper().get(policy.getAction());
            // exception if null?
            if (action != null) {
                resourcePolicy.setAction(action);
            }

            final String type = policy.getType();
            if (type != null) {
                resourcePolicy.setRpType(type);
            }

            resourcePolicies.add(resourcePolicy);
        }

        authorizeService.removeAllPolicies(Curator.curationContext(), dSpaceObject);
        authorizeService.addPolicies(Curator.curationContext(), resourcePolicies, dSpaceObject);
    }

    /**
     * This is pretty light so just recreate it so it can be gc'd later
     *
     * @return the mapping between action String and Int representations
     */
    private static BiMap<String, Integer> actionMapper() {
        return ImmutableBiMap.<String, Integer>builder()
                      .put("ADD", Constants.ADD)
                      .put("READ", Constants.READ)
                      .put("ADMIN", Constants.ADMIN)
                      .put("WRITE", Constants.WRITE)
                      .put("DELETE", Constants.DELETE)
                      .put("REMOVE", Constants.REMOVE)
                      .put("READ_ITEM", Constants.DEFAULT_ITEM_READ)
                      .put("READ_BITSTREAM", Constants.DEFAULT_BITSTREAM_READ)
            .build();
    }

}
