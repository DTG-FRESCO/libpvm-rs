# Automated project semantic versioning (semver.org) based on git tags.
#
# template: M.m.p-lbl.delta+g[commit-hash][.d]
#
# Consider that on branch master, HEAD points to commit with hash 5e8f33, while
# master~2 is a commit you have tagged with the name v0.5.32-rc1. In this
# imaginary setup, the tag is the one closest to the current HEAD which starts
# with v followed by a number (that's how "version" tags are recognized by the
# build script).
#
# The current project version is determined by filling in the template fields
# based on the closest git tag.
#
# Matching the template with the example tag given above:
# - M is the major version (0 in example above),
# - m is the minor version (5)
# - p is the patch version (32)
# - lbl is an optional pre-release label either determined by the user, set to
#   the part in the git tag following the patch version or is the name of the
#   current branch (default when no other option is given) (rc1)
# - delta is the number of commits from the tag to HEAD (2 in the example, as
#   the tag points at master~2)
# - g[commit-hash] is the literal g (from "git") followed by the commit-hash
#   at HEAD
# - .d is present if the build was done in a dirty git tree
#
# The semver string in this case would be:
#
# 0.5.32-rc1.2+g5e8f33
#
# - if you want to override the label (lbl), uncomment the
#   set(PROJECT_TAG_VERSION) statement below and set it to something like
#   "beta", "dev", "rel"; leave it commented out to use the current git branch
#   name if no other label is present in the git tag (default)
#
# #Releases
#
# If a version tag exists pointing at the current HEAD, that is considered to
# be a release and no delta or git hash components are added. The label appears
# only if explicitly user set or if it exists in the tag (no branch names added)
#
function(semver dirty_marker tag_overwrite)
execute_process(
  COMMAND git --git-dir ${CMAKE_SOURCE_DIR}/.git describe --match v[0-9]* --dirty=.${dirty_maker} --tags
  OUTPUT_VARIABLE GIT_DESC
  )
execute_process(
  COMMAND git --git-dir ${CMAKE_SOURCE_DIR}/.git rev-parse --abbrev-ref HEAD
  OUTPUT_VARIABLE GIT_BRANCH
  )
string(REPLACE "\n" "" GIT_BRANCH ${GIT_BRANCH})
string(REGEX MATCH "v([0-9]+)\\.([0-9]+)\\.([0-9]+)(-([^0-9-]+))?(-([0-9]*)-g([0-9a-f.]*))?"
                    GIT_DESC_PARSE ${GIT_DESC})
if(DEFINED CMAKE_MATCH_1)
  set(PROJECT_MAJOR_VERSION ${CMAKE_MATCH_1})
endif()
if(DEFINED CMAKE_MATCH_2)
  set(PROJECT_MINOR_VERSION ${CMAKE_MATCH_2})
endif()
if(DEFINED CMAKE_MATCH_3)
  set(PROJECT_PATCH_VERSION ${CMAKE_MATCH_3})
endif()
if(DEFINED CMAKE_MATCH_5)
  set(PROJECT_GIT_TAG_LABEL ${CMAKE_MATCH_5})
endif()
if(DEFINED CMAKE_MATCH_7)
  set(PROJECT_GIT_TAG_DELTA ${CMAKE_MATCH_7})
endif()
if(DEFINED CMAKE_MATCH_8)
  set(PROJECT_GIT_COMMIT_HASH ${CMAKE_MATCH_8})
endif()

## Manual setting for PROJECT_TAG_VERSION. Edit this when needed
#
# Manually set the tag version if needed; the default is taken from the tag if
# it exists, otherwise it is set to the current branch name
#
#set(PROJECT_TAG_VERSION "rel")

# libpvm utilities version.
set(PROJECT_UTILS_VERSION 1)

if(("${PROJECT_TAG_VERSION}" STREQUAL "") AND (NOT "${PROJECT_GIT_TAG_LABEL}" STREQUAL ""))
  set(PROJECT_TAG_VERSION ${PROJECT_GIT_TAG_LABEL})
endif()
# if not set already, set PROJECT_TAG_VERSION to the git branch
if("${PROJECT_TAG_VERSION}" STREQUAL "")
  set(PROJECT_TAG_VERSION ${GIT_BRANCH})
  set(IGNORE_TAG_FOR_RELEASE 1)
endif()

if("${PROJECT_GIT_COMMIT_HASH}" STREQUAL "")
  #this is a release as the current commit points to a tag!
  if("${PROJECT_GIT_TAG_LABEL}" STREQUAL "")
    if(DEFINED IGNORE_TAG_FOR_RELEASE)
      set(PROJECT_VERSION ${PROJECT_MAJOR_VERSION}.${PROJECT_MINOR_VERSION}.${PROJECT_PATCH_VERSION})
    else()
      set(PROJECT_VERSION ${PROJECT_MAJOR_VERSION}.${PROJECT_MINOR_VERSION}.${PROJECT_PATCH_VERSION}-${PROJECT_TAG_VERSION})
    endif()
  else()
    set(PROJECT_VERSION ${PROJECT_MAJOR_VERSION}.${PROJECT_MINOR_VERSION}.${PROJECT_PATCH_VERSION}-${PROJECT_TAG_VERSION})
  endif()
else()
  set(PROJECT_VERSION ${PROJECT_MAJOR_VERSION}.${PROJECT_MINOR_VERSION}.${PROJECT_PATCH_VERSION}-${PROJECT_TAG_VERSION}.${PROJECT_GIT_TAG_DELTA}+${PROJECT_GIT_COMMIT_HASH})
endif()

endfunction(semver)

